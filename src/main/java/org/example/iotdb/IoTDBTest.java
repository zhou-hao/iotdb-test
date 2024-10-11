package org.example.iotdb;

import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

public class IoTDBTest {

    public static void main(String[] args) throws Exception {

        int port = startIotDB();

        try (Session session = new Session.Builder()
                .host("127.0.0.1")
                .port(port)
                .build()) {

            session.open();

            Tablet tablet = new Tablet(
                    "root.db1.test",
                    Collections.singletonList(new MeasurementSchema("temp", TSDataType.FLOAT)),
                    1);

            tablet.addTimestamp(0, 1);
            tablet.addValue("temp", 0, 1.0F);

            session.insertTablet(tablet);

            SessionDataSet set = session.executeQueryStatement("select * from root.db1.test");
            if(!set.hasNext()){
                throw new IllegalStateException();
            }
        }

    }

    private static int startIotDB() {
        GenericContainer<?> container = new GenericContainer<>(
                DockerImageName.parse("apache/iotdb:1.3.2-standalone"))
                .withEnv("TZ", "Asia/Shanghai")
                .withEnv("cn_internal_address", "localhost")
                .withEnv("cn_internal_port", "10710")
                .withEnv("cn_consensus_port", "10720")
                .withEnv("dn_internal_address", "localhost")
                .withEnv("dn_rpc_port", "6667")
                .withEnv("dn_rpc_address", "0.0.0.0")
                .withEnv("dn_internal_port", "10730")
                .withEnv("dn_mpp_data_exchange_port", "10740")
                .withEnv("dn_schema_region_consensus_port", "10750")
                .withEnv("dn_data_region_consensus_port", "10760")
                .withEnv("dn_seed_config_node", "localhost:10710")
                .withPrivilegedMode(true)
                .withLogConsumer(data -> {
                    try {
                        System.out.write(data.getBytes());
                    } catch (IOException e) {
                    }
                })
                .withExposedPorts(6667)
                .waitingFor(Wait.forListeningPort())
//             .waitingFor(Wait.forLogMessage(".*IoTDB DataNode is set up successfully.*",1))
                ;
        container.start();

        return container.getMappedPort(6667);
    }
}
