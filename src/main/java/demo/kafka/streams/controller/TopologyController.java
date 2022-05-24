package demo.kafka.streams.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/v1/kafka-streams")
public class TopologyController {

    @Autowired
    private StreamsBuilderFactoryBean factoryBean;

    /**
     * Endpoint providing a description of the topology.
     */
    @GetMapping("/topology")
    public ResponseEntity<String> getTopology() {
        return ResponseEntity.ok(factoryBean.getTopology().describe().toString());
    }
}
