/**
 * Put your copyright and license info here.
 */
package com.example.idempotencyTester;

import org.apache.apex.malhar.kafka.KafkaSinglePortInputOperator;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.lib.io.ConsoleOutputOperator;

@ApplicationAnnotation(name="IdempotencyTester")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    // Sample DAG with 2 operators
    // Replace this code with the DAG you want to build

    KafkaSinglePortInputOperator kafka = dag.addOperator("Kafka", KafkaSinglePortInputOperator.class);
    kafka.setClusters("localhost:9093");
    kafka.setTopics("test");

    IdempotencyTesterOperator tester = dag.addOperator("Tester", new IdempotencyTesterOperator());

    dag.addStream("stream", kafka.outputPort, tester.input);
  }
}
