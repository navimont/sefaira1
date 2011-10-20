<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<%@ page import="com.google.appengine.api.datastore.DatastoreServiceFactory" %>
<%@ page import="com.google.appengine.api.datastore.DatastoreService" %>
<%@ page import="com.google.appengine.api.datastore.Entity" %>
<%@ page import="com.google.appengine.api.datastore.EntityNotFoundException" %>
<%@ page import="com.google.appengine.api.datastore.Key" %>
<%@ page import="com.google.appengine.api.datastore.KeyFactory" %>

<html>
  <body>

    <h1>Lookup values in a specific range</h1>   
<%
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
%>

  <form action="/lookup" method="get">
	
	 <p>Search through <%= fileIndex %> datasets using 
	  	<select name="ntasks" size="1">
	  		<option>1</option>
	  		<option>2</option>
	  		<option>3</option>
	  		<option>4</option>
	  		<option>5</option>
	  		<option>6</option>
	  		<option>7</option>
	  		<option>8</option>
	  		<option>9</option>
	  		<option>10</option>
	  	</select>
	  parallel tasks.</p>
	  
	  <h2>Specify variable names</h2>  
	  
	    <div>
	    	Search for variable <input type="text" name="var1"/>
	    	in range from <input type="text" name="var1_min">
	    	to <input type="text" name="var1_max">
	    </div>
	    <div>
	    	Search for variable <input type="text" name="var2"/>
	    	in range from <input type="text" name="var2_min">
	    	to <input type="text" name="var2_max">
	    </div>
	    <div><input type="submit" value="Start search" /></div>
  </form>
  
  <div id="task-list">
  </div>
  
  </body>
</html>