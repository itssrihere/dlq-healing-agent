package com.example.dlqagent.infrastructure;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 * A build-time guard for the second layer of the cross-site defence. StateChangingRequestGuard already refuses any
 * state-changing request that is not application/json, whatever the controllers say. The controllers also declare
 * {@code consumes = application/json}, so that a request of the wrong type is rejected by Spring MVC itself and the rule is visible in
 * the code that handles the request. This test makes that second layer impossible to forget: it reads the mapping annotations of
 * every controller in the application the way Spring does and fails the build if an endpoint that can change state does not declare
 * exactly {@code application/json}. It needs no Spring context, so a new controller is picked up automatically.
 */
class ControllerContractTest {

    private static final Set<RequestMethod> CHANGES_STATE =
            EnumSet.of(RequestMethod.POST, RequestMethod.PUT, RequestMethod.PATCH, RequestMethod.DELETE);

    /** Endpoints of {@code controller} that can change state but do not declare consumes = application/json. */
    static List<String> violations(Class<?> controller) {
        RequestMapping onClass = AnnotatedElementUtils.findMergedAnnotation(controller, RequestMapping.class);
        List<String> found = new ArrayList<>();
        for (Method handler : controller.getDeclaredMethods()) {
            RequestMapping mapping = AnnotatedElementUtils.findMergedAnnotation(handler, RequestMapping.class);
            if (mapping == null) {
                continue;
            }
            Set<RequestMethod> methods = EnumSet.noneOf(RequestMethod.class);
            methods.addAll(Arrays.asList(mapping.method()));
            if (methods.isEmpty() && onClass != null) {
                methods.addAll(Arrays.asList(onClass.method()));
            }
            boolean anyMethod = methods.isEmpty(); // no method restriction means POST is accepted too
            if (!anyMethod && methods.stream().noneMatch(CHANGES_STATE::contains)) {
                continue; // a read
            }
            // as in Spring MVC, a method-level consumes replaces the class-level one
            String[] consumes = mapping.consumes().length > 0 ? mapping.consumes() : onClass != null ? onClass.consumes() : new String[0];
            if (!(consumes.length == 1 && MediaType.APPLICATION_JSON_VALUE.equals(consumes[0]))) {
                found.add(controller.getSimpleName() + "." + handler.getName() + " accepts " + (anyMethod ? "every HTTP method" : methods)
                        + " but declares consumes=" + Arrays.toString(consumes));
            }
        }
        return found;
    }

    /** The application's own controllers: main classes only, so the deliberately forgetful probes in other tests are not scanned. */
    private static List<Class<?>> applicationControllers() throws ClassNotFoundException {
        var scanner = new ClassPathScanningCandidateComponentProvider(false);
        scanner.addIncludeFilter(new AnnotationTypeFilter(RestController.class));
        scanner.addIncludeFilter(new AnnotationTypeFilter(Controller.class));
        List<Class<?>> controllers = new ArrayList<>();
        for (BeanDefinition candidate : scanner.findCandidateComponents("com.example.dlqagent")) {
            Class<?> type = Class.forName(candidate.getBeanClassName());
            String location = type.getProtectionDomain().getCodeSource().getLocation().getPath();
            if (!location.contains("test-classes")) {
                controllers.add(type);
            }
        }
        return controllers;
    }

    @Test
    void everyEndpointThatCanChangeStateDeclaresJson() throws Exception {
        List<Class<?>> controllers = applicationControllers();

        assertThat(controllers).as("the scan must find the application's controllers, or it proves nothing")
                .extracting(Class::getSimpleName).contains("ApprovalController", "AuditController");
        List<String> problems = new ArrayList<>();
        controllers.forEach(c -> problems.addAll(violations(c)));
        assertThat(problems).as("add consumes = MediaType.APPLICATION_JSON_VALUE (StateChangingRequestGuard would refuse the request "
                + "anyway, but the rule should be visible where the request is handled)").isEmpty();
    }

    // ---- the checker itself, against hand-written samples -------------------------------------------------------------

    static class Forgetful {
        @PostMapping("/x")
        void change() {}
    }

    static class WrongType {
        @PostMapping(path = "/x", consumes = MediaType.TEXT_PLAIN_VALUE)
        void change() {}
    }

    static class AcceptsEveryMethod {
        @RequestMapping("/x")
        void anything() {}
    }

    static class AlsoAcceptsForms {
        @PostMapping(path = "/x", consumes = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_FORM_URLENCODED_VALUE})
        void change() {}
    }

    static class ReadsOnly {
        @GetMapping("/x")
        void read() {}

        @RequestMapping(path = "/y", method = {RequestMethod.GET, RequestMethod.HEAD})
        void readToo() {}
    }

    static class Declares {
        @PostMapping(path = "/x", consumes = MediaType.APPLICATION_JSON_VALUE)
        void change() {}

        @RequestMapping(path = "/y", method = RequestMethod.DELETE, consumes = "application/json")
        void remove() {}
    }

    @RequestMapping(consumes = MediaType.APPLICATION_JSON_VALUE)
    static class DeclaresOnTheClass {
        @PostMapping("/x")
        void change() {}
    }

    @RequestMapping(consumes = MediaType.APPLICATION_JSON_VALUE)
    static class MethodLevelReplacesClassLevel {
        @PostMapping(path = "/x", consumes = MediaType.TEXT_PLAIN_VALUE)
        void change() {}
    }

    @Test
    void theCheckerFlagsWhatItShouldAndOnlyThat() {
        assertThat(violations(Forgetful.class)).singleElement().asString().contains("Forgetful.change").contains("consumes=[]");
        assertThat(violations(WrongType.class)).hasSize(1);
        assertThat(violations(AcceptsEveryMethod.class)).singleElement().asString().contains("every HTTP method");
        assertThat(violations(AlsoAcceptsForms.class)).as("a form type next to JSON reopens the hole").hasSize(1);
        assertThat(violations(MethodLevelReplacesClassLevel.class)).as("as in Spring, the method's consumes wins").hasSize(1);
        assertThat(violations(ReadsOnly.class)).isEmpty();
        assertThat(violations(Declares.class)).isEmpty();
        assertThat(violations(DeclaresOnTheClass.class)).isEmpty();
    }
}
