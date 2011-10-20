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

public class LookupServlet extends HttpServlet {
    private static final Logger log = Logger.getLogger(LookupServlet.class.getName());
   
    @SuppressWarnings("unchecked")
	public void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws IOException {
    	Gson gson = new Gson();
    	String vartasks = "";
    	Map<String,Object> results = new HashMap<String,Object>();
    	
        Cache cache;
        try {
            CacheFactory cacheFactory = CacheManager.getInstance().getCacheFactory();
            cache = cacheFactory.createCache(Collections.emptyMap());
        } catch (CacheException e) {
            log.severe("Failed to access memcache");
            return;
        }    	     		
        if (cache.containsKey("vartasks")) {
        	vartasks = (String) cache.get("vartasks");        	
        }

		// go through list of tasks and encode task status
    	List<Object> tasklist = new LinkedList<Object>();
        if (!vartasks.isEmpty()) {
			for (String task : vartasks.split(", ")) {
	        	Map<String,Object> taskstatus = new HashMap<String,Object>();
				taskstatus.put("name", task);
				log.info("Task: " + task + " status: " + cache.get(task) + " time: "+cache.get(task+"_time"));
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

    	// get variable names and range parameters from request
    	String var1 = req.getParameter("var1");
    	String var2 = req.getParameter("var2");  	
    	if (var1 == null || var1.isEmpty()) {
    		log.warning("var1 parameter is empty.");    		
    	}
    	if (var2 == null || var2.isEmpty()) {
    		log.warning("var2 parameter is empty.");    		
    	}
    	List<Object> vars = new LinkedList<Object>();
    	vars.add(var1);
    	vars.add(var2);
    	results.put("vars", vars);
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
    	List<Object> data = new LinkedList<Object>();
    	for (long file=1; file <= fileIndex; file++) {
    		if (cache.containsKey("file"+file)) {
    			Map<String,Float> varval = (Map) cache.get("file"+file);
    			// check range
    			if (varval.containsKey(var1) && varval.containsKey(var2)) {
    				float val1 = (float) varval.get(var1);
    				float val2 = (float) varval.get(var2);
    				if (val1 > var1_min && val1 < var1_max && val2 > var2_min && val2 < var2_max ) {
    					List<Float> vv = new LinkedList<Float>();
    					vv.add(val1);
    					vv.add(val2);
    					data.add(vv);
    				}
    			}		
    		}
    	}
    	results.put("data", data);
    	
    	// encode result and go
        resp.setContentType("text/plain");
        resp.getWriter().println(gson.toJson(results));		
		
        // two less because that much space is used for the task status encoding
		log.info("Returned " + (results.size()-2) + " result value pairs.");
    }
}