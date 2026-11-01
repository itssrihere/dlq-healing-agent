package com.example.dlqagent.infrastructure;

import com.example.dlqagent.domain.ContractSource;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Component;

/** Reads a contract from any Spring resource location, for example {@code classpath:contracts/orders.schema.json}. */
@Component
class ResourceContractSource implements ContractSource {

    private final ResourceLoader loader;

    ResourceContractSource(ResourceLoader loader) {
        this.loader = loader;
    }

    @Override
    public String load(String location) {
        try {
            return loader.getResource(location).getContentAsString(StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot read contract '" + location + "'", e);
        }
    }
}
