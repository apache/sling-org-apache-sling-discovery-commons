/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.sling.discovery.commons.providers.base;

import java.io.Serializable;
import java.util.Date;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.sling.commons.scheduler.Job;
import org.apache.sling.commons.scheduler.JobContext;
import org.apache.sling.commons.scheduler.ScheduleOptions;
import org.apache.sling.commons.scheduler.Scheduler;

public class DummyScheduler implements Scheduler {

    class DummyScheduleOptions implements ScheduleOptions {

        private String name;
        private Map<String, Serializable> config;
        private Date date;

        @Override
        public ScheduleOptions config(Map<String, Serializable> config) {
            this.config = config;
            return this;
        }

        @Override
        public ScheduleOptions name(String name) {
            this.name = name;
            return this;
        }

        @Override
        public ScheduleOptions canRunConcurrently(boolean flag) {
            return this;
        }

        @Override
        public ScheduleOptions onLeaderOnly(boolean flag) {
            return this;
        }

        @Override
        public ScheduleOptions onSingleInstanceOnly(boolean flag) {
            return this;
        }

        @Override
        public ScheduleOptions onInstancesOnly(String[] slingIds) {
            return this;
        }

        public ScheduleOptions date(Date date) {
            this.date = date;
            return this;
        }

        @Override
        public ScheduleOptions threadPoolName(String name) {
            this.name = name;
            return this;
        }
    }

    private boolean failMode;

    @Override
    public void addJob(String name, Object job, Map<String, Serializable> config, String schedulingExpression,
            boolean canRunConcurrently) throws Exception {
        throw new IllegalStateException("not yet impl");
    }

    @Override
    public void addPeriodicJob(String name, Object job, Map<String, Serializable> config, long period, boolean canRunConcurrently)
            throws Exception {
        throw new IllegalStateException("not yet impl");
    }

    @Override
    public void addPeriodicJob(String name, Object job, Map<String, Serializable> config, long period, boolean canRunConcurrently,
            boolean startImmediate) throws Exception {
        throw new IllegalStateException("not yet impl");
    }

    @Override
    public void fireJob(Object job, Map<String, Serializable> config) throws Exception {
        throw new IllegalStateException("not yet impl");
    }

    @Override
    public boolean fireJob(Object job, Map<String, Serializable> config, int times, long period) {
        throw new IllegalStateException("not yet impl");
    }

    @Override
    public void fireJobAt(final String name, final Object job, final Map<String, Serializable> config, final Date date) throws Exception {
        if (!(job instanceof Job) && !(job instanceof Runnable)) {
            throw new IllegalArgumentException("only runnable and job supported");
        }
        Runnable r = new Runnable() {

            @Override
            public void run() {
                while (System.currentTimeMillis()<date.getTime()) {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        Thread.yield();
                    }
                }
                if (job instanceof Job) {
                    Job j = (Job)job;
                    JobContext context = new JobContext() {

                        @Override
                        public String getName() {
                            return name;
                        }

                        @Override
                        public Map<String, Serializable> getConfiguration() {
                            return config;
                        }
                    };
                    j.execute(context);
                } else {
                    ((Runnable)job).run();
                }
            }
            
        };
        async(r, name);
    }

    private void async(Runnable r, String name) {
        if (failMode) {
            throw new IllegalStateException("failMode");
        }
        Thread th = new Thread(r);
        th.setName("async test thread for "+name);
        th.setDaemon(true);
        th.start();
    }

    @Override
    public boolean fireJobAt(String name, Object job, Map<String, Serializable> config, Date date, int times, long period) {
        throw new IllegalStateException("not yet impl");
    }

    @Override
    public void removeJob(String name) throws NoSuchElementException {
        throw new IllegalStateException("not yet impl");
    }

    public void failMode() {
        failMode = true;
    }

    @Override
    public boolean schedule(Object job, ScheduleOptions options) {
        DummyScheduleOptions dOptions = (DummyScheduleOptions) options;
        try {
            fireJobAt(dOptions.name, job, dOptions.config, dOptions.date);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return true;
    }

    @Override
    public boolean unschedule(String jobName) {
        throw new IllegalStateException("not yet impl");
    }

    @Override
    public ScheduleOptions NOW() {
        return new DummyScheduleOptions().date(new Date());
    }

    @Override
    public ScheduleOptions NOW(int times, long period) {
        throw new IllegalStateException("not yet impl");
    }

    @Override
    public ScheduleOptions AT(Date date) {
        return new DummyScheduleOptions().date(date);
    }

    @Override
    public ScheduleOptions AT(Date date, int times, long period) {
        throw new IllegalStateException("not yet impl");
    }

    @Override
    public ScheduleOptions EXPR(String expression) {
        throw new IllegalStateException("not yet impl");
    }

}
