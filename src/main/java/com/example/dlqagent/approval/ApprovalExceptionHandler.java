package com.example.dlqagent.approval;

import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

@RestControllerAdvice(assignableTypes = ApprovalController.class)
class ApprovalExceptionHandler {

    @ExceptionHandler(ApprovalNotFoundException.class)
    ResponseEntity<Map<String, String>> notFound(ApprovalNotFoundException e) {
        return error(HttpStatus.NOT_FOUND, e.getMessage());
    }

    @ExceptionHandler(ApprovalConflictException.class)
    ResponseEntity<Map<String, String>> conflict(ApprovalConflictException e) {
        return error(HttpStatus.CONFLICT, e.getMessage());
    }

    @ExceptionHandler(ReplayFailedException.class)
    ResponseEntity<Map<String, String>> replayFailed(ReplayFailedException e) {
        return error(HttpStatus.BAD_GATEWAY, e.getMessage());
    }

    @ExceptionHandler({MethodArgumentNotValidException.class, HttpMessageNotReadableException.class,
            MethodArgumentTypeMismatchException.class})
    ResponseEntity<Map<String, String>> badRequest(Exception e) {
        return error(HttpStatus.BAD_REQUEST, "Invalid request");
    }

    private static ResponseEntity<Map<String, String>> error(HttpStatus status, String message) {
        return ResponseEntity.status(status).body(Map.of("error", message));
    }
}
