package mapred;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Text;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;	
import java.lang.Float;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import net.sf.jsr107cache.Cache;
import net.sf.jsr107cache.CacheException;
import net.sf.jsr107cache.CacheFactory;
import net.sf.jsr107cache.CacheManager;

/**
 * @author Stefan Wehner (2011)
 *
 * The class is responsible to serve lookup requests. It returns the 
 * status of the background tasks and the results so far. Response
 * is a JSON structure:
 * {
 *   "tasks": [ { "name":   <task name>
 *                "status": <task status RUNNING|FINISHED>
 *                "time":   <task run time>
 *              }
 *              ...
 *            ]
 *   "series": [ { "name": "<var1> vs. <var2>"
 *                 "data": [[<value1_1>, <value2_1>] ... [<value1_n>, <value2_n>]]
 *               ...
 *             ]
 * } 
 */
public class LookupServlet extends HttpServlet {
    private static final Logger log = Logger.getLogger(LookupServlet.class.getName());
   
    @SuppressWarnings("unchecked")
	public void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws IOException {
    	Gson gson = new Gson();
    	String vartasks = "";
    	Map<String,Object> results = new HashMap<String,Object>();
    	boolean tasks_active = false;
    	
    	// get variable names and range parameters from request
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

    	Cache cache;
        try {
            CacheFactory cacheFactory = CacheManager.getInstance().getCacheFactory();
            cache = cacheFactory.createCache(Collections.emptyMap());
        } catch (CacheException e) {
            log.severe("Failed to access memcache");
            return;
        }    	     		
        String vartaskKey = "vartasks?var1="+var1+"&var2="+var2;
        if (cache.containsKey(vartaskKey)) {
        	vartasks = (String) cache.get(vartaskKey);        	
        }

		// go through list of tasks and encode task status
    	List<Object> tasklist = new LinkedList<Object>();
        if (!vartasks.isEmpty()) {
			for (String task : vartasks.split(", ")) {
	        	Map<String,Object> taskstatus = new HashMap<String,Object>();
				taskstatus.put("name", task);
				log.info("Task: " + task + " status: " + cache.get(task) + " time: "+cache.get(task+"_time"));
				if ("RUNNING".equals(cache.get(task))) {
					tasks_active = true;
				}
				if (cache.containsKey(task)) {
					taskstatus.put("status", cache.get(task));
				} else {
					taskstatus.put("status", "<invalid status>");
				}
				if (cache.containsKey(task+"_time")) {
					taskstatus.put("time", (Float) cache.get(task+"_time"));
				} else {
					taskstatus.put("time", "<no time info>");
				}
				tasklist.add(taskstatus);
			}
        }
    	results.put("tasks", tasklist);
      	results.put("active", tasks_active);

     	float var1_min = Float.MIN_VALUE;
        try {
        	 var1_min = Float.valueOf(req.getParameter("var1_min"));
        } catch (NumberFormatException e) {
        } catch (NullPointerException e) {}
    	float var1_max = Float.MAX_VALUE;
        try {
        	 var1_max = Float.valueOf(req.getParameter("var1_max"));
        } catch (NumberFormatException e) {
        } catch (NullPointerException e) {}
    	float var2_min = Float.MIN_VALUE;
        try {
        	 var2_min = Float.valueOf(req.getParameter("var2_min"));
        } catch (NumberFormatException e) {
        } catch (NullPointerException e) {}
    	float var2_max = Float.MAX_VALUE;
        try {
        	 var2_max = Float.valueOf(req.getParameter("var2_max"));
        } catch (NumberFormatException e) {
        } catch (NullPointerException e) {}
    	            	
		// go through memcache and collect results
    	List<Object> series = new LinkedList<Object>();
    	Map<String,Object> serie_map = new HashMap<String,Object>();
    	serie_map.put("name", var1+" vs. "+var2);
    	List<List> data = new LinkedList<List>();
    	// go over the tasks and look in the cache for the batches they stored there
    	for (String task : vartasks.split(", ")) {
    		int batch_counter = 1;
    		while (cache.containsKey(task+"_cache_"+batch_counter)) {
    			List<List> cachedata = (List) cache.get(task+"_cache_"+batch_counter);
    			// go over value pairs in the list and check range
    			for (List<Float> pair : cachedata) {
    				float val1 = (float) pair.get(0);
    				float val2 = (float) pair.get(1);
    				if (val1 > var1_min && val1 < var1_max && val2 > var2_min && val2 < var2_max ) {
    					// add pair to output list
    					data.add(pair);
    				}
    			}	
    			batch_counter++;
    		}
    	}
    	serie_map.put("data", data);
    	series.add(serie_map);
    	results.put("series", series);
    	
    	// encode result and go
        resp.setContentType("text/plain");
        resp.getWriter().println(gson.toJson(results));		
		
		log.info("Returned " + (data.size()) + " result value pairs.");
    }
}