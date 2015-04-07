/**
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.corfudb.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.http.impl.client.HttpClients;
import org.apache.http.client.HttpClient;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;

import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.Json;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * Note, the following imports require Java 8
 */
import java.util.concurrent.locks.StampedLock;

/**
 * This class is used by clients to access the CorfuDB infrastructure.
 * It is responsible for constructing client views, and returning
 * interfaces which clients use to access the CorfuDB infrastructure.
 *
 * @author Michael Wei <mwei@cs.ucsd.edu>
 */
public class CorfuDBClient implements java.io.Serializable  {

    private String configurationString;
    private StampedLock viewLock;
    private Thread viewManagerThread;
    private CorfuDBView currentView;
    private BooleanLock viewUpdatePending;

    private Logger log = LoggerFactory.getLogger(CorfuDBClient.class);

    private class BooleanLock
    {
        public boolean lock;
        public BooleanLock() {
            lock = false;
        }
    }

    /**
     * Suppressed default constructor.
     */
    private CorfuDBClient() {}

    /**
     * Constructor. Generates an instance of a CorfuDB client, which
     * manages views of the CorfuDB infrastructure and provides interfaces
     * for clients to access.
     *
     * @param configurationString   A configuration string which describes how to reach the \
     *                              CorfuDB instance. This is usually a http address for a \
     *                              configuration master.
     */
    public CorfuDBClient(String configurationString) {
        this.configurationString = configurationString;

        viewLock = new StampedLock();
        viewUpdatePending = new BooleanLock();
        viewUpdatePending.lock = true;
        viewManagerThread = getViewManagerThread();
    }

    /**
     * Starts the view manager thread. The view manager retrieves the view
     * and manages view changes. This thread will be automatically started
     * when any requests are made, but this method allows the view manager
     * to load the inital view during application load.
     */
    public void startViewManager() {
        if (!viewManagerThread.isAlive())
        {
            log.debug("Starting view manager thread.");
            viewManagerThread.start();
        }
    }

    /**
     * Retrieves the CorfuDBView from a configuration string. The view manager
     * uses this method to fetch the most recent view.
     */
    private CorfuDBView retrieveView()
        throws IOException
    {
        HttpClient httpClient = HttpClients.createDefault();
        HttpResponse response = httpClient.execute(new HttpGet(configurationString));
        if (response.getStatusLine().getStatusCode() != 200)
        {
            log.warn("Failed to get view from configuration string", response.getStatusLine());
            throw new IOException("Couldn't get view from configuration string");
        }
        try (JsonReader jr = Json.createReader(new BufferedReader(new InputStreamReader(response.getEntity().getContent()))))
        {
            return new CorfuDBView(jr.readObject());
        }
    }

    /**
     * Invalidate the current view and wait for a new view.
     */
    public void invalidateViewAndWait()
    {
        log.warn("Client requested invalidation of current view");
        if (currentView != null)
        {
            currentView.invalidate();
        }
            synchronized(viewUpdatePending)
            {
                viewUpdatePending.lock = true;
                viewUpdatePending.notify();
                while (viewUpdatePending.lock)
                {
                    try {
                    viewUpdatePending.wait();
                    } catch (InterruptedException ie)
                    {}
                }
            }
    }

    /**
     * Synchronously block until a valid view is installed.
     */
    public void waitForViewReady()
    {
        synchronized(viewUpdatePending)
        {
            while (viewUpdatePending.lock)
            {
                try {
                viewUpdatePending.wait();
                } catch (InterruptedException ie)
                {
                	
                }
            }
        }
    }

    /**
     * Get the current view. This method optimisically acquires the
     * current view.
     */
    public CorfuDBView getView()
    {
        if (viewManagerThread == null || currentView == null || !currentView.isValid())
        {
            if (viewManagerThread == null)
            {
                startViewManager();
            }
            synchronized(viewUpdatePending)
            {
                while (viewUpdatePending.lock)
                {
                    try {
                    viewUpdatePending.wait();
                    } catch (InterruptedException ie)
                    {}
                }
            }
        }
        long stamp = viewLock.tryOptimisticRead();
        CorfuDBView view = currentView;
        if (!viewLock.validate(stamp))
        {
            //We should only get here if the view is being updated.
            stamp = viewLock.readLock();
            view = currentView;
            viewLock.unlock(stamp);
        }
        return currentView;
    }

    /**
     * Retrieves a runnable that provides a view manager thread. The view
     * manager retrieves the view and manages view changes.
     */
    private Thread getViewManagerThread() {
        return new Thread(new Runnable() {
            @Override
            public void run() {
                log.debug("View manager thread started.");
                while (true)
                {
                    synchronized(viewUpdatePending)
                    {
                        if (viewUpdatePending.lock)
                        {
                            log.debug("View manager retrieving view...");
                            //lock, preventing old view from being read.
                            long stamp = viewLock.writeLock();
                            try {
                                CorfuDBView newView = retrieveView();
                                if (currentView == null || newView.getEpoch() > currentView.getEpoch());
                                {
                                    String oldEpoch = (currentView == null) ? "null" : Long.toString(currentView.getEpoch());
                                    log.info("New view epoch " + newView.getEpoch() + " greater than old view epoch " + oldEpoch + ", changing views");
                                    currentView = newView;
                                    viewUpdatePending.lock = false;
                                    viewUpdatePending.notifyAll();
                                }
                            }
                            catch (IOException ie)
                            {
                                log.warn("Error retrieving view: " + ie.getMessage());
                                currentView.invalidate();
                            }
                            finally {
                                viewLock.unlock(stamp);
                            }
                        }
                        else
                        {
                            while (!viewUpdatePending.lock)
                            {
                                try {
                                    viewUpdatePending.wait();
                                }
                                catch (InterruptedException ie){}
                            }
                        }
                    }
                }
            }
        });
    }
}
