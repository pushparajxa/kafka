/*
 * Copyright (c) 2022 D. E. Shaw & Co., L.P. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of D. E. Shaw & Co., L.P. ("Confidential Information")
 */

package deshaw.middleware.connector;

import deshaw.common.util.LogFactory;
import deshaw.middleware.connector.DesRestServer;

import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.connect.runtime.Herder;

import java.net.URI;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

/**
 * This class ties together all the components of a Kafka Connect process
 * (herder, worker, storage, command interface), managing their lifecycle.
 * This class is exactly same as
 * {@link org.apache.kafka.connect.runtime.Connect}. 
 */
public class DesConnect
{
    private static final Logger log = LogFactory.getLogger(DesConnect.class);

    private final Herder herder;
    private final DesRestServer rest;
    private final CountDownLatch startLatch = new CountDownLatch(1);
    private final CountDownLatch stopLatch = new CountDownLatch(1);
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final ShutdownHook shutdownHook;

    public DesConnect(Herder herder, DesRestServer rest)
    {
        log.fine("Kafka Connect instance created");
        this.herder = herder;
        this.rest = rest;
        shutdownHook = new ShutdownHook();
    }

    public void start() 
    {
        try {
            log.info("Kafka Connect starting");
            Exit.addShutdownHook("connect-shutdown-hook", shutdownHook);

            herder.start();
            rest.initializeResources(herder);

            log.info("Kafka Connect started");
        } finally {
            startLatch.countDown();
        }
    }

    public void stop() 
    {
        try {
            boolean wasShuttingDown = shutdown.getAndSet(true);
            if (!wasShuttingDown) {
                log.info("Kafka Connect stopping");

                rest.stop();
                herder.stop();

                log.info("Kafka Connect stopped");
            }
        } finally {
            stopLatch.countDown();
        }
    }

    public void awaitStop()
    {
        try {
            stopLatch.await();
        } catch (InterruptedException e) {
            log.severe("Interrupted waiting for Kafka Connect to shutdown");
        }
    }

    public boolean isRunning() 
    {
        return herder.isRunning();
    }

    // Visible for testing
    public URI restUrl() 
    {
        return rest.serverUrl();
    }

    public URI adminUrl() 
    {
        return rest.adminUrl();
    }

    private class ShutdownHook extends Thread
    {
        @Override
        public void run() 
        {
            try {
                startLatch.await();
                DesConnect.this.stop();
            } catch (InterruptedException e) {
                log.severe("Interrupted in shutdown hook while waiting for " +
                                "Kafka Connect startup to finish");
            }
        }
    }
}

