package mapred;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.TaskHandle;
import com.google.appengine.api.taskqueue.TaskOptions;

import static com.google.appengine.api.taskqueue.TaskOptions.Builder.*;

import java.lang.Long;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import net.sf.jsr107cache.Cache;
import net.sf.jsr107cache.CacheException;
import net.sf.jsr107cache.CacheFactory;
import net.sf.jsr107cache.CacheManager;
import com.google.appengine.api.memcache.jsr107cache.GCacheFactory;


/**
 * @author Stefan Wehner (2011)
 *
 * The servlet is called with the form data from lookup.jsp and is responsible 
 * to start the background tasks which will do the actual data lookup. 
 */
public class MapServlet extends HttpServlet {
    private static final Logger log =
            Logger.getLogger(MapServlet.class.getName());
   
    public void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws IOException {
        long ntasks = 3L;
        
        // retrieve number of background tasks to employ for the search
        try {
        	ntasks = Long.valueOf(req.getParameter("ntasks"));
        } catch (NumberFormatException e) {}
        ntasks = ntasks > 10 ? 10 : ntasks;
        ntasks = ntasks < 1 ? 1 : ntasks;
        
        // get the variable names to lookup
    	String var1 = req.getParameter("var1");
    	String var2 = req.getParameter("var2");  	
    	if (var1 == null || var1.isEmpty()) {
    		log.warning("var1 parameter is empty.");    	
    		var1="";
    	}
    	if (var2 == null || var2.isEmpty()) {
    		log.warning("var2 parameter is empty.");    		
    		var2="";
    	}
        // note: the input range will be determined by the follow up calls 
    	// to retrieve the results
    	
        // read current number of entities in datastore
        DatastoreService datastore =  DatastoreServiceFactory.getDatastoreService();
    	Entity counter;
    	long fileIndex;
		Key counterKey = KeyFactory.createKey("Counter", "filecounter");
		try {
			counter = datastore.get(counterKey);
	    	fileIndex = (Long) counter.getProperty("total_files");			
    	} catch (EntityNotFoundException e) {
    		fileIndex = 0L;
    	}         
    	// just to make sure we don't employ too many workers
    	ntasks = ntasks > fileIndex ? fileIndex : ntasks;

        Cache cache;
        try {
            CacheFactory cacheFactory = CacheManager.getInstance().getCacheFactory();
            Map<Object,Object> props = new HashMap<Object,Object>();
            props.put(GCacheFactory.EXPIRATION_DELTA, 3600);
            cache = cacheFactory.createCache(props);
        } catch (CacheException e) {
            log.severe("Failed access memcache");
            return;
        }    	     		

        // start first task after some time to make sure the system is still responsive
        // and will create the lookup.jsp page after finishing this servlet.
	    long deferred = 600;

    	// divide the work between the number of tasks specified
    	if (fileIndex != 0 && var1 != null && var2 != null) {
    		// map will store the names of the strings in the queue and their status
    		String tasksInQ = "";
    		long portion = fileIndex / ntasks;
    		for (int tx = 0; tx < ntasks; tx++) {
    			long start = portion * tx +1;
    			long end = portion * (tx+1);
    			// compensate for cut off decimals in last portion
    			if (tx == ntasks-1) {
    				end = fileIndex;
    			}
    			// enqueue background task
    		    Queue queue = QueueFactory.getDefaultQueue();
    		    TaskOptions options = withUrl("/vartask");
    		    options.method(TaskOptions.Method.GET);
    		    options.param("var1", var1);
    		    options.param("var2", var2);
    		    options.param("start_index", ""+start);
    		    options.param("end_index", ""+end);
    		    // start every task a little later
    		    deferred += 100;
    		    options.countdownMillis(deferred);
    		    TaskHandle vartask = queue.add(options);    	
    		    if (tasksInQ.isEmpty()) {
        			tasksInQ = vartask.getName();    		    	
    		    } else {
    		    	tasksInQ = tasksInQ + ", " + vartask.getName();
    		    }
    			log.info("Task " + vartask.getName() + " in queue to work on index " + start + " to " + end);
    		}
            cache.put("vartasks?var1="+var1+"&var2="+var2, tasksInQ);
    	}

    	// this is just needed to pass back the settings for rendering the next page
    	// (which is the same page from where we came)
     	float var1_min = -50.0F;
        try {
        	 var1_min = Float.valueOf(req.getParameter("var1_min"));
        } catch (NumberFormatException e) {
        } catch (NullPointerException e) {}
    	float var1_max = 50.0F;
        try {
        	 var1_max = Float.valueOf(req.getParameter("var1_max"));
        } catch (NumberFormatException e) {
        } catch (NullPointerException e) {}
    	float var2_min = -50.0F;
        try {
        	 var2_min = Float.valueOf(req.getParameter("var2_min"));
        } catch (NumberFormatException e) {
        } catch (NullPointerException e) {}
    	float var2_max = 50.0F;
        try {
        	 var2_max = Float.valueOf(req.getParameter("var2_max"));
        } catch (NumberFormatException e) {
        } catch (NullPointerException e) {}
    	
        resp.sendRedirect("/lookup.jsp?ntasks="+ntasks+"&var1="+var1+"&var2="+var2+"&var1_min="+var1_min+"&var1_max="+var1_max+"&var2_min="+var2_min+"&var2_max="+var2_max);
    }
}