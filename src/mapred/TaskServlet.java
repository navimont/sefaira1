package mapred;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
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
 * Background task loads batches of data in the given index range.
 * It extracts the variables in question and stores them in 
 * memcache. 
 */
public class TaskServlet extends HttpServlet {
    private static final Logger log = Logger.getLogger(TaskServlet.class.getName());
   
    @SuppressWarnings("unchecked")
	public void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws IOException {
    	Gson gson = new Gson();
        String taskname = req.getHeader("X-AppEngine-TaskName");
    	
        long start_time = System.currentTimeMillis();
        
        Cache cache;
        try {
            CacheFactory cacheFactory = CacheManager.getInstance().getCacheFactory();
            cache = cacheFactory.createCache(Collections.emptyMap());
            cache.put(taskname, "RUNNING");
            cache.put(taskname+"_time", (Float) ((System.currentTimeMillis()-start_time)/1000.0F));
        } catch (CacheException e) {
            log.severe("Failed to access memcache");
            return;
        }    	     		
		log.info("Task " + taskname  + " is RUNNING.");
    	
        // get the variable names to lookup
    	String var1 = req.getParameter("var1");
    	String var2 = req.getParameter("var2");
    	// and the range of subsequent file id's 
    	int startIndex = Integer.valueOf(req.getParameter("start_index"));
    	int endIndex = Integer.valueOf(req.getParameter("end_index"));
    	
    	// this is the parent key needed for file lookup
		Key counterKey = KeyFactory.createKey("Counter", "filecounter");
        DatastoreService datastore =  DatastoreServiceFactory.getDatastoreService();
    	// collect keys in a list for a batch get
    	List<Key> keys = new LinkedList<Key>();
    	Text textdata;
    	// will contain the JSON decoded representation of an entities data block
    	Map<String,Float> varmap;
    	// will contain a batch result from the datastore
    	Map<Key,Entity> datamap;
    	// use this list to store data in the cache
        List<List> cachedata = new LinkedList<List>();
    	// needed to parse json data
		Type mapType = new TypeToken<Map<String,Float>>() {}.getType();
		// counts the number of batches we retrieve form the datastore and then place in a cache 
		int batches_counter = 0;
    	for (int findex = startIndex; findex <= endIndex; findex++) {
    		keys.add(KeyFactory.createKey(counterKey, "jsondata", findex));
    		if ( (findex % 30) == 0 || findex == endIndex) {
    			batches_counter++;
    			datamap = datastore.get(keys);
                cache.put(taskname+"_time", (Float) ((System.currentTimeMillis()-start_time)/1000.0F));
                // retrieve all entries from the datamap
    			for (Map.Entry<Key,Entity> entry : datamap.entrySet()) {
	    			textdata = (Text) entry.getValue().getProperty("data");
	    			varmap = gson.fromJson(textdata.getValue(), mapType);
	    			List<Float> pair = new ArrayList<Float>(2);
	    			pair.add(0, varmap.get(var1));
	    			pair.add(1, varmap.get(var2));
	    			cachedata.add(pair);
	    		}
    			// store the data portion in cache.
    			cache.put(taskname+"_cache_"+batches_counter, cachedata);
    			log.info("Stored "+cachedata.size()+" value pairs in cache: "+taskname+"_cache_"+batches_counter);
        		keys.clear();
        		cachedata.clear();
    		}
    	}

		// update status in memcache
        cache.put(taskname, "FINISHED");
        cache.put(taskname+"_time", (Float) ((System.currentTimeMillis()-start_time)/1000.0F));
        
		log.info("Task " + req.getHeader("X-AppEngine-TaskName") + " looked up datasets from "+startIndex+" to "+endIndex+" and FINISHED after "+(System.currentTimeMillis()-start_time)/1000.0+" s");
    }
}