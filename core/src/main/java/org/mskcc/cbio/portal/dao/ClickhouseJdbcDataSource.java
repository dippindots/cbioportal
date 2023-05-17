package org.mskcc.cbio.portal.dao;

import org.apache.commons.dbcp2.BasicDataSource;
import org.mskcc.cbio.portal.util.GlobalProperties;

public class ClickhouseJdbcDataSource extends BasicDataSource {
    
    public ClickhouseJdbcDataSource() {
        this.setDriverClassName("com.clickhouse.jdbc.ClickHouseDriver");
        this.setUsername("cbio");
        this.setPassword("P@ssword1");
        this.setUrl("jdbc:clickhouse://localhost:8123/cbioportal");
        this.setJmxName("org.cbioportal:DataSource=clickhouse");
    }
}
