/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.chinacloud.processors.demo;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.hive.HiveDBCPService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.hive.AbstractHiveQLProcessor;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"sql", "hive", "create", "database", "external", "table","chinacloud"})
@CapabilityDescription("Executes a HiveQL DDL command (create). The content of an incoming FlowFile is expected to be the HiveQL command "
        + "to execute. The HiveQL command may use the ? to escape parameters. In this case, the parameters to use must exist as FlowFile attributes "
        + "with the naming convention hiveql.args.N.type and hiveql.args.N.value, where N is a positive integer. The hiveql.args.N.type is expected to be "
        + "a number indicating the JDBC Type. The content of the FlowFile is expected to be in UTF-8 format.")
@ReadsAttributes({
        @ReadsAttribute(attribute = "hive.ddl", description = "The create statement."),
        @ReadsAttribute(attribute = "absolute.hdfs.path", description = "The location of the data file.")
})
public class CreateHiveExternalTable extends AbstractHiveQLProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile is routed to this relationship after the table is created")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship if the table can not be created")
            .build();

    private static List<PropertyDescriptor> descriptors;

    private static Set<Relationship> relationships;
    private byte[] buffer;

    /*
     * Will ensure that the list of property descriptors is build only once.
     * Will also create a Set of relationships
     */
    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.add(HIVE_DBCP_SERVICE);
        descriptors = Collections.unmodifiableList(_propertyDescriptors);

        Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(_relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final List<FlowFile> flowFiles = session.get(1000);
        final ComponentLog logger = getLogger();
        if (flowFiles.isEmpty()) {
            return;
        }
        final HiveDBCPService dbcpService = context.getProperty(HIVE_DBCP_SERVICE).asControllerService(HiveDBCPService.class);
        try (final Connection conn = dbcpService.getConnection()) {
            for (FlowFile flowFile : flowFiles) {
                Map<String, String> attributes = flowFile.getAttributes();
                String create_sql = attributes.get("hive.ddl") + " location '" + attributes.get("absolute.hdfs.path") + "' ";
                logger.error(create_sql);
                final Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("show databases");
                while(rs.next()){
                    logger.error(rs.getString(1));
                }
                session.transfer(flowFile, REL_SUCCESS);
                break;
            }
        } catch (final SQLException sqle) {
            // There was a problem getting the connection, yield and retry the flowfiles
            getLogger().error("Failed to get Hive connection due to {}", new Object[]{sqle});
            session.transfer(flowFiles, REL_FAILURE);
            context.yield();
        }
    }
}
