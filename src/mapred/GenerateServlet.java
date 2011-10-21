package mapred;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Text;
import com.google.appengine.api.datastore.Transaction;
import com.google.appengine.api.users.User;
import com.google.appengine.api.users.UserService;
import com.google.appengine.api.users.UserServiceFactory;
import com.google.gson.Gson;

import java.lang.Integer;
import java.lang.Long;
import java.lang.Float;
import java.lang.NumberFormatException;
import java.io.IOException;
import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.HashMap;
import java.util.Random;
import java.util.logging.Logger;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * @author Stefan Wehner (2011)
 *
 * Generate datasets with random test data. A counter entity
 * is kept in the datastore and updated in a transaction with the
 * new or deleted data sets. 
 */
public class GenerateServlet extends HttpServlet {
    private static final Logger log =
            Logger.getLogger(GenerateServlet.class.getName());

    private static Map<String,Float> generateData(int len) {
    	Map<String, Float> datalist = new HashMap<String, Float>();
    	Random rand = new Random();
    	
    	int i;
    	for (i=0; i<len; i++) {
    		datalist.put(String.format("VAR%03d", i), (rand.nextFloat()-0.5F)*100.0F);
    	}
    	
    	return datalist;
    }
    
    public void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws IOException {
    	Gson gson = new Gson();
        Integer number = 0;
        
        // retrieve the number of new data sets to create in the DB
        try {
        	number = Integer.valueOf(req.getParameter("number"));
        } catch (NumberFormatException e) {}
        // not more than 100 at a time
        number = number > 500 ? 500 : number;
        number = number < -500 ? -500 : number;
        
        // read current number of entities in datastore
        DatastoreService datastore =  DatastoreServiceFactory.getDatastoreService();
    	Entity counter;
    	Long fileIndex;
		Key counterKey = KeyFactory.createKey("Counter", "filecounter");
		try {
			counter = datastore.get(counterKey);
	    	fileIndex = (Long) counter.getProperty("total_files");			
    	} catch (EntityNotFoundException e) {
    		fileIndex = 0L;
    	}         
                
        if (number != 0) {
            Transaction txn = datastore.beginTransaction();
            
        	try {
            	List<Entity> jsondataList = new LinkedList<Entity>();
            	try {
            		counter = datastore.get(counterKey);
                	fileIndex = (Long) counter.getProperty("total_files");
            	} catch (EntityNotFoundException e) {
            		// first usage of the storage. Create the element.
            		counter = new Entity(counterKey);
            		fileIndex = 0L;
            	}
            	
            	if (number > 0) {
            		// create new datasets
	            	while (number > 0) {	        	
	    		        fileIndex++;
	    		        number--;
	            		// counter is a parent, needed for write in transaction. The file index is part of the key
	            		Key jsondataKey = KeyFactory.createKey(counterKey, "jsondata", fileIndex);
	    		        Entity jsondata = new Entity(jsondataKey);
	    		        // generate a json dataset with key-value pairs
	    		        Text jsontext = new Text(gson.toJson(generateData(300)));
	    		        jsondata.setProperty("data", jsontext);
	    		        jsondataList.add(jsondata);
	            	}
	                counter.setUnindexedProperty("total_files", fileIndex);
	                log.info(jsondataList.size() + " datasets ready for commit. Last file index is " + fileIndex);
	            	datastore.put(counter);
	                datastore.put(jsondataList);
            	} else {
            		// delete data
                	List<Key> keys = new LinkedList<Key>();
	            	while (number < 0 && fileIndex > 0) {	        	
	    		        number++;
	            		// counter is a parent, needed for write in transaction. The file index is part of the key
	    		        keys.add(KeyFactory.createKey(counterKey, "jsondata", fileIndex));
	    		        fileIndex--;
	            	}
	                counter.setUnindexedProperty("total_files", fileIndex);
	                log.info(""+keys.size()+" datasets ready for delete. Last file index is now " + fileIndex);
	            	datastore.put(counter);
	                datastore.delete(keys);
            	}
                txn.commit();
        	} finally {
        		if (txn.isActive()) {
        			txn.rollback();
        		}
        	}
        }

        resp.sendRedirect("/generate.jsp");
    }
}