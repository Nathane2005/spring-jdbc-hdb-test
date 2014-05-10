/**
 * Copyright 2013 Andrew Clemons <andrew.clemons@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nz.geek.caffe.spring.hdb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.PermissionDeniedDataAccessException;
import org.springframework.dao.UncategorizedDataAccessException;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.InvalidResultSetAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.LazyConnectionDataSourceProxy;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.jdbc.support.SQLErrorCodeSQLExceptionTranslator;

/**
 * Tests for SQL error codes on SAP HANA. Some of the tests are duplicated for
 * both row and column store. I don't think HANA uses different error codes for
 * them, but this just makes certain. <br />
 * Reference: <a href=
 * "http://help.sap.com/saphelp_hanaplatform/helpdata/en/20/a78d3275191014b41bae7c4a46d835/content.htm"
 * >SQL Error Codes</a>
 *
 * @author <a href='mailto:andrew.clemons@gmail.com'>Andrew Clemons</a>
 */
public class HANAExceptionMappingTest {

    private static SingleConnectionDataSource datasource;

    /**
     * init.
     */
    @BeforeClass
    public static void staticInit() {
	datasource = new SingleConnectionDataSource(System.getProperty(
		"jdbcUrl", "jdbc:sap://localhost:30115"), System.getProperty(
		"jdbcUser", "spring"), System.getProperty("jdbcPassword",
		"spring"), true);
    }

    /**
     * tear down.
     */
    @AfterClass
    public static void staticTearDown() {
	datasource.destroy();
    }

    private ExecutorService executor;

    private JdbcTemplate jdbcTemplate;

    private void dropTables() {
	try {
	    this.jdbcTemplate.execute("drop table TEST_CHILD_COL cascade");
	} catch (final DataAccessException e) {
	    // ignored
	}

	try {
	    this.jdbcTemplate.execute("drop table TEST_PARENT_COL cascade");
	} catch (final DataAccessException e) {
	    // ignored
	}

	try {
	    this.jdbcTemplate.execute("drop table TEST_CHILD_ROW cascade");
	} catch (final DataAccessException e) {
	    // ignored
	}

	try {
	    this.jdbcTemplate.execute("drop table TEST_PARENT_ROW cascade");
	} catch (final DataAccessException e) {
	    // ignored
	}
    }

    /**
     * Setup.
     */
    @Before
    public void setup() {
	this.executor = Executors.newCachedThreadPool();

	this.jdbcTemplate = new JdbcTemplate(datasource);

	dropTables();

	this.jdbcTemplate
		.execute("create column table TEST_PARENT_COL(ID_ int, STR_ varchar(10) not null, primary key (ID_))");
	this.jdbcTemplate
		.execute("create column table TEST_CHILD_COL(ID_ int, STR_ varchar(10) not null, PARENT_ int not null, primary key (ID_))");

	this.jdbcTemplate
		.execute("alter table TEST_CHILD_COL add constraint FK_PARENT_CHILD_COL foreign key (PARENT_) references TEST_PARENT_COL(ID_)");

	this.jdbcTemplate
		.execute("create table TEST_PARENT_ROW(ID_ int, STR_ varchar(10) not null, primary key (ID_))");
	this.jdbcTemplate
		.execute("create table TEST_CHILD_ROW(ID_ int, STR_ varchar(10) not null, PARENT_ int not null, primary key (ID_))");

	this.jdbcTemplate
		.execute("alter table TEST_CHILD_ROW add constraint FK_PARENT_CHILD_ROW foreign key (PARENT_) references TEST_PARENT_ROW(ID_)");
    }

    /**
     * Tear down.
     */
    @After
    public void tearDown() {
	this.executor.shutdownNow();

	dropTables();

    }

    /**
     */
    @Test
    public void testCreateDuplicateIndexColumnStore() {
	this.jdbcTemplate
		.execute("CREATE INDEX IDX1_COL ON TEST_PARENT_COL(STR_)");

	try {
	    this.jdbcTemplate
		    .execute("CREATE INDEX IDX1_COL ON TEST_PARENT_COL(STR_)");
	    Assert.fail("create index should have failed");
	} catch (final BadSqlGrammarException e) {
	    // expected
	}
    }

    /**
     */
    @Test
    public void testCreateDuplicateIndexRowStore() {
	this.jdbcTemplate
		.execute("CREATE INDEX IDX1_ROW ON TEST_PARENT_ROW(STR_)");

	try {
	    this.jdbcTemplate
		    .execute("CREATE INDEX IDX1_ROW ON TEST_PARENT_ROW(STR_)");
	    Assert.fail("create index should have failed");
	} catch (final BadSqlGrammarException e) {
	    // expected
	}
    }

    /**
     */
    @Test
    public void testCreateDuplicateTableColumnStore() {
	try {
	    this.jdbcTemplate
		    .execute("create column table TEST_PARENT_COL(ID_ int, STR_ varchar(10) not null, primary key (ID_))");
	    Assert.fail("create table should have failed");
	} catch (final BadSqlGrammarException e) {
	    // expected
	}
    }

    /**
     */
    @Test
    public void testCreateDuplicateTableRowStore() {
	try {
	    this.jdbcTemplate
		    .execute("create column table TEST_PARENT_ROW(ID_ int, STR_ varchar(10) not null, primary key (ID_))");
	    Assert.fail("create table should have failed");
	} catch (final BadSqlGrammarException e) {
	    // expected
	}
    }

    /**
     */
    @Test
    public void testCreateIndexReservedKeywordColumnStore() {
	try {
	    this.jdbcTemplate
		    .execute("CREATE INDEX CURSOR ON TEST_PARENT_COL(STR_)");
	    Assert.fail("create index should have failed");
	} catch (final BadSqlGrammarException e) {
	    // expected
	}
    }

    /**
     */
    @Test
    public void testCreateIndexReservedKeywordRowStore() {
	try {
	    this.jdbcTemplate
		    .execute("CREATE INDEX CURSOR ON TEST_PARENT_ROW(STR_)");
	    Assert.fail("create index should have failed");
	} catch (final BadSqlGrammarException e) {
	    // expected
	}
    }

    /**
     */
    @Test
    public void testCreateTableReservedColumnKeywordColumnStore() {
	try {
	    this.jdbcTemplate
		    .execute("CREATE COLUMN TABLE RESERVED_TEST (CURSOR varchar(10) not null)");
	    Assert.fail("create table should have failed");
	} catch (final BadSqlGrammarException e) {
	    // expected
	}
    }

    /**
     */
    @Test
    public void testCreateTableReservedColumnKeywordRowStore() {
	try {
	    this.jdbcTemplate
		    .execute("CREATE TABLE RESERVED_TEST (CURSOR varchar(10) not null)");
	    Assert.fail("create table should have failed");
	} catch (final BadSqlGrammarException e) {
	    // expected
	}
    }

    /**
     */
    @Test
    public void testCreateTableReservedKeywordColumnStore() {
	try {
	    this.jdbcTemplate
		    .execute("CREATE COLUMN TABLE CURSOR (STR_ varchar(10) not null)");
	    Assert.fail("create table should have failed");
	} catch (final BadSqlGrammarException e) {
	    // expected
	}
    }

    /**
     */
    @Test
    public void testCreateTableReservedKeywordRowStore() {
	try {
	    this.jdbcTemplate
		    .execute("CREATE TABLE CURSOR (STR_ varchar(10) not null)");
	    Assert.fail("create table should have failed");
	} catch (final BadSqlGrammarException e) {
	    // expected
	}
    }

    /**
     */
    @Test
    public void testDropUnknownColumnColumnStore() {
	try {
	    this.jdbcTemplate.execute("ALTER TABLE TEST_PARENT_COL DROP STR__");
	    Assert.fail("alter table should have failed");
	} catch (final BadSqlGrammarException e) {
	    // expected
	}
    }

    /**
     */
    @Test
    public void testDropUnknownColumnRowStore() {
	try {
	    this.jdbcTemplate.execute("ALTER TABLE TEST_PARENT_ROW DROP STR__");
	    Assert.fail("alter table should have failed");
	} catch (final BadSqlGrammarException e) {
	    // expected
	}
    }

    /**
     */
    @Test
    public void testDropUnknownIndex() {
	try {
	    this.jdbcTemplate.execute("DROP INDEX BLIBB");
	    Assert.fail("drop index should have failed");
	} catch (final BadSqlGrammarException e) {
	    // expected
	}
    }

    /**
     */
    @Test
    public void testDropUnknownTable() {
	try {
	    this.jdbcTemplate.execute("DROP TABLE BLUBB");
	    Assert.fail("drop table should have failed");
	} catch (final BadSqlGrammarException e) {
	    // expected
	}
    }

    /**
     */
    @Test
    public void testForeignKeyConstraintViolationColumnStore() {
	try {
	    this.jdbcTemplate
		    .execute("INSERT INTO TEST_CHILD_COL VALUES (1, 'test', 2)");
	    Assert.fail("Insert should have failed");
	} catch (final DataIntegrityViolationException e) {
	    // expected
	}
    }

    /**
     */
    @Test
    public void testForeignKeyConstraintViolationRowStore() {
	try {
	    this.jdbcTemplate
		    .execute("INSERT INTO TEST_CHILD_ROW VALUES (1, 'test', 2)");
	    Assert.fail("Insert should have failed");
	} catch (final DataIntegrityViolationException e) {
	    // expected
	}
    }

    /**
     */
    @Test
    public void testInsertValueTooLongColumnStore() {
	try {
	    this.jdbcTemplate
		    .execute("INSERT INTO TEST_PARENT_COL VALUES (1, 'testtesttest')");
	    Assert.fail("Insert should have failed");
	} catch (final UncategorizedDataAccessException e) {
	    // expected
	}
    }

    /**
     */
    @Test
    public void testInsertValueTooLongRowStore() {
	try {
	    this.jdbcTemplate
		    .execute("INSERT INTO TEST_PARENT_ROW VALUES (1, 'testtesttest')");
	    Assert.fail("Insert should have failed");
	} catch (final UncategorizedDataAccessException e) {
	    // expected
	}
    }

    /**
     */
    @Test
    public void testInsertWrongColCountColumnStore() {
	try {
	    this.jdbcTemplate
		    .execute("INSERT INTO TEST_CHILD_COL VALUES (1, 'test')");
	    Assert.fail("Insert should have failed");
	} catch (final BadSqlGrammarException e) {
	    // expected
	}
    }

    /**
     */
    @Test
    public void testInsertWrongColCountColumnStore2() {
	try {
	    this.jdbcTemplate
		    .execute("INSERT INTO TEST_CHILD_COL VALUES (1, 'test', 2, 2)");
	    Assert.fail("Insert should have failed");
	} catch (final BadSqlGrammarException e) {
	    // expected
	}
    }

    /**
     */
    @Test
    public void testInsertWrongColCountRowStore() {
	try {
	    this.jdbcTemplate
		    .execute("INSERT INTO TEST_CHILD_ROW VALUES (1, 'test')");
	    Assert.fail("Insert should have failed");
	} catch (final BadSqlGrammarException e) {
	    // expected
	}
    }

    /**
     */
    @Test
    public void testInsertWrongColCountRowStore2() {
	try {
	    this.jdbcTemplate
		    .execute("INSERT INTO TEST_CHILD_ROW VALUES (1, 'test', 2, 2)");
	    Assert.fail("Insert should have failed");
	} catch (final BadSqlGrammarException e) {
	    // expected
	}
    }

    /**
     */
    @Test
    public void testInvalidResultSetColumnColumnStore() {
	this.jdbcTemplate
		.execute("INSERT INTO TEST_PARENT_COL VALUES (1, 'test')");

	try {
	    this.jdbcTemplate.query(new PreparedStatementCreator() {

		public PreparedStatement createPreparedStatement(Connection con)
			throws SQLException {
		    return con
			    .prepareStatement("SELECT STR_ FROM TEST_PARENT_COL");
		}
	    }, new ResultSetExtractor<String>() {

		public String extractData(final ResultSet rs)
			throws SQLException, DataAccessException {

		    return rs.getString("STR__");
		}
	    });
	    Assert.fail("query should have failed");
	} catch (final InvalidResultSetAccessException e) {
	    // expected
	}
    }

    /**
     */
    @Test
    public void testInvalidResultSetColumnColumnStore2() {
	this.jdbcTemplate
		.execute("INSERT INTO TEST_PARENT_COL VALUES (1, 'test')");

	try {
	    this.jdbcTemplate.query(new PreparedStatementCreator() {

		public PreparedStatement createPreparedStatement(Connection con)
			throws SQLException {
		    return con
			    .prepareStatement("SELECT STR_ FROM TEST_PARENT_COL");
		}
	    }, new ResultSetExtractor<String>() {

		public String extractData(final ResultSet rs)
			throws SQLException, DataAccessException {

		    return rs.getString(7);
		}
	    });
	    Assert.fail("query should have failed");
	} catch (final InvalidResultSetAccessException e) {
	    // expected
	}
    }

    /**
     */
    @Test
    public void testInvalidResultSetColumnRowStore() {
	this.jdbcTemplate
		.execute("INSERT INTO TEST_PARENT_ROW VALUES (1, 'test')");

	try {
	    this.jdbcTemplate.query(new PreparedStatementCreator() {

		public PreparedStatement createPreparedStatement(Connection con)
			throws SQLException {
		    return con
			    .prepareStatement("SELECT STR_ FROM TEST_PARENT_ROW");
		}
	    }, new ResultSetExtractor<String>() {

		public String extractData(final ResultSet rs)
			throws SQLException, DataAccessException {

		    return rs.getString("STR__");
		}
	    });
	    Assert.fail("query should have failed");
	} catch (final InvalidResultSetAccessException e) {
	    // expected
	}
    }

    /**
     */
    @Test
    public void testInvalidResultSetColumnRowStore2() {
	this.jdbcTemplate
		.execute("INSERT INTO TEST_PARENT_ROW VALUES (1, 'test')");

	try {
	    this.jdbcTemplate.query(new PreparedStatementCreator() {

		public PreparedStatement createPreparedStatement(Connection con)
			throws SQLException {
		    return con
			    .prepareStatement("SELECT STR_ FROM TEST_PARENT_ROW");
		}
	    }, new ResultSetExtractor<String>() {

		public String extractData(final ResultSet rs)
			throws SQLException, DataAccessException {

		    return rs.getString(7);
		}
	    });
	    Assert.fail("query should have failed");
	} catch (final InvalidResultSetAccessException e) {
	    // expected
	}
    }

    /**
     */
    @Test
    public void testPermissionDenied() {
	try {
	    this.jdbcTemplate.execute("drop user "
		    + System.getProperty("jdbcUser", "spring") + " cascade");
	    Assert.fail("drop user should have failed");
	} catch (final PermissionDeniedDataAccessException e) {
	    // expected
	}
    }

    /**
     */
    @Test
    public void testUnableToConnect() {
	final DriverManagerDataSource ds = new DriverManagerDataSource(
		"jdbc:sap://blahhost:30115", "blah", "blah");

	JdbcTemplate template = new JdbcTemplate();
	template.setExceptionTranslator(new SQLErrorCodeSQLExceptionTranslator(
		"HDB"));
	template.setDataSource(new LazyConnectionDataSourceProxy(ds));
	template.afterPropertiesSet();

	try {
	    template.execute("SELECT 1 FROM DUMMY");

	    Assert.fail("connect should have failed");
	} catch (final DataAccessResourceFailureException e) {
	    // expected
	}
    }

    /**
     */
    @Test
    public void testUnknownColumnColumnStore() {
	this.jdbcTemplate
		.execute("INSERT INTO TEST_PARENT_COL VALUES (1, 'test2')");

	try {
	    this.jdbcTemplate
		    .execute("UPDATE TEST_PARENT_COL SET STR__ =  'test2' WHERE ID_ = 1");
	    Assert.fail("Insert should have failed");
	} catch (final BadSqlGrammarException e) {
	    // expected
	}
    }

    /**
     */
    @Test
    public void testUnknownColumnRowStore() {
	this.jdbcTemplate
		.execute("INSERT INTO TEST_PARENT_ROW VALUES (1, 'test2')");

	try {
	    this.jdbcTemplate
		    .execute("UPDATE TEST_PARENT_ROW SET STR__ =  'test2' WHERE ID_ = 1");
	    Assert.fail("Update should have failed");
	} catch (final BadSqlGrammarException e) {
	    // expected
	}
    }

    /**
     */
    @Test
    public void testUnknownTableColumnStore() {
	this.jdbcTemplate
		.execute("INSERT INTO TEST_PARENT_COL VALUES (1, 'test2')");

	try {
	    this.jdbcTemplate
		    .execute("UPDATE TEST_PARENT_COL_ SET STR_ =  'test2' WHERE ID_ = 1");
	    Assert.fail("Update should have failed");
	} catch (final BadSqlGrammarException e) {
	    // expected
	}
    }

    /**
     */
    @Test
    public void testUnknownTableRowStore() {
	this.jdbcTemplate
		.execute("INSERT INTO TEST_PARENT_ROW VALUES (1, 'test2')");

	try {
	    this.jdbcTemplate
		    .execute("UPDATE TEST_PARENT_ROW_ SET STR_ =  'test2' WHERE ID_ = 1");
	    Assert.fail("Update should have failed");
	} catch (final BadSqlGrammarException e) {
	    // expected
	}
    }

    /**
     */
    @Test
    public void testValueDuplicateKeyColumnStore() {
	this.jdbcTemplate
		.execute("INSERT INTO TEST_PARENT_COL VALUES (1, 'test')");

	try {
	    this.jdbcTemplate
		    .execute("INSERT INTO TEST_PARENT_COL VALUES (1, 'test2')");
	    Assert.fail("Insert should have failed");
	} catch (final DuplicateKeyException e) {
	    // expected
	}
    }

    /**
     */
    @Test
    public void testValueDuplicateRowStore() {
	this.jdbcTemplate
		.execute("INSERT INTO TEST_PARENT_ROW VALUES (1, 'test')");

	try {
	    this.jdbcTemplate
		    .execute("INSERT INTO TEST_PARENT_ROW VALUES (1, 'test2')");
	    Assert.fail("Insert should have failed");
	} catch (final DuplicateKeyException e) {
	    // expected
	}
    }

    /**
     */
    @Test
    public void testWrongDataTypeColumnStore() {
	try {
	    this.jdbcTemplate
		    .execute("CREATE COLUMN TABLE BLAH (STR_ NUMERIC(100))");
	    Assert.fail("create table should have failed");
	} catch (final BadSqlGrammarException e) {
	    // expected
	}
    }

    /**
     */
    @Test
    public void testWrongDataTypeRowStore() {
	try {
	    this.jdbcTemplate.execute("CREATE TABLE BLAH (STR_ NUMERIC(100))");
	    Assert.fail("create table should have failed");
	} catch (final BadSqlGrammarException e) {
	    // expected
	}
    }

    /**
     */
    @Test
    public void testForeignKeyConstraintViolationOnDeleteColumnStore() {
	this.jdbcTemplate
		.execute("INSERT INTO TEST_PARENT_COL VALUES (1, 'test')");
	this.jdbcTemplate
		.execute("INSERT INTO TEST_CHILD_COL VALUES (1, 'test', 1)");

	try {
	    this.jdbcTemplate.execute("DELETE FROM TEST_PARENT_COL");
	    Assert.fail("delete should have failed");
	} catch (final DataIntegrityViolationException e) {
	    // expected
	}
    }

    /**
     */
    @Test
    public void testForeignKeyConstraintViolationOnDeleteRowStore() {
	this.jdbcTemplate
		.execute("INSERT INTO TEST_PARENT_ROW VALUES (1, 'test')");
	this.jdbcTemplate
		.execute("INSERT INTO TEST_CHILD_ROW VALUES (1, 'test', 1)");

	try {
	    this.jdbcTemplate.execute("DELETE FROM TEST_PARENT_ROW");
	    Assert.fail("delete should have failed");
	} catch (final DataIntegrityViolationException e) {
	    // expected
	}
    }

    /**
     */
    @Test
    public void testForeignKeyConstraintViolationOnUpdateColumnStore() {
	this.jdbcTemplate
		.execute("INSERT INTO TEST_PARENT_COL VALUES (1, 'test')");
	this.jdbcTemplate
		.execute("INSERT INTO TEST_CHILD_COL VALUES (1, 'test', 1)");

	try {
	    this.jdbcTemplate.execute("UPDATE TEST_CHILD_COL set PARENT_ = 2");
	    Assert.fail("update should have failed");
	} catch (final DataIntegrityViolationException e) {
	    // expected
	}
    }

    /**
     */
    @Test
    public void testForeignKeyConstraintViolationOnUpdateRowStore() {
	this.jdbcTemplate
		.execute("INSERT INTO TEST_PARENT_ROW VALUES (1, 'test')");
	this.jdbcTemplate
		.execute("INSERT INTO TEST_CHILD_ROW VALUES (1, 'test', 1)");

	try {
	    this.jdbcTemplate.execute("UPDATE TEST_CHILD_ROW set PARENT_ = 2");
	    Assert.fail("update should have failed");
	} catch (final DataIntegrityViolationException e) {
	    // expected
	}
    }

    /**
     */
    @Test
    public void testInvalidDateRowStore() {
	this.jdbcTemplate.execute("CREATE TABLE BLAH (TIMESTAMP_ TIMESTAMP)");

	try {
	    this.jdbcTemplate.execute("INSERT INTO BLAH VALUES ('2013-11-21 0:90:00')");
	    Assert.fail("insert should have failed");
	} catch (final DataIntegrityViolationException e) {
	    // expected
	} finally {
	    this.jdbcTemplate.execute("DROP TABLE BLAH");
	}
    }
}
