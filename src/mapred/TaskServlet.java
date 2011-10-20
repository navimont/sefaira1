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
    	Map<String,Float> varmap;
    	Map<Key,Entity> datamap;
    	// needed to parse json data
		Type mapType = new TypeToken<Map<String,Float>>() {}.getType();
		// used to store vars in memcache
		Map<String,Float> reduced_var_map = new HashMap<String,Float>();
    	for (int findex = startIndex; findex <= endIndex; findex++) {
    		keys.add(KeyFactory.createKey(counterKey, "jsondata", findex));
    		if ( (findex % 10) == 0 || findex == endIndex) {
    			datamap = datastore.get(keys);
    			for (Map.Entry<Key,Entity> entry : datamap.entrySet()) {
	    			textdata = (Text) entry.getValue().getProperty("data");
	    			varmap = gson.fromJson(textdata.getValue(), mapType);
	    			// store vars in memcache with the file index as memcache key
	    			reduced_var_map.put(var1, varmap.get(var1));
	    			reduced_var_map.put(var2, varmap.get(var2));
	    			// log.info("file"+entry.getKey().getId()+" var1: "+ reduced_var_map.get(var1) + " var2: " + reduced_var_map.get(var2));
	    			cache.put("file"+entry.getKey().getId(), reduced_var_map);
	    		}
        		keys.clear();
    		}
    	}

		// update status in memcache
        cache.put(taskname, "FINISHED");
        long end_time = System.currentTimeMillis();
        cache.put(taskname+"_time", (Float) ((end_time-start_time)/1000.0F));
        
		log.info("Task " + req.getHeader("X-AppEngine-TaskName") + " has FINISHED after "+(end_time-start_time)/1000.0+" s");
    }
}