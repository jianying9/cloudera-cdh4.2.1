package com.wolf.mapred;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author aladdin
 */
public class TestJobStartJUnitTest {

    public TestJobStartJUnitTest() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    @Test
    public void test() throws Exception {
//        String[] args = {};
//        Configuration config = HBaseConfiguration.create();
//        int res = ToolRunner.run(config, new TestJobStart(), args);
//        System.exit(res);
        TestJobStart jobStart = new TestJobStart();
        String name = jobStart.getParameter("name");
        System.out.println("name:" + name);
    }
}