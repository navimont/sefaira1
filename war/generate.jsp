<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<%@ page import="com.google.appengine.api.datastore.DatastoreServiceFactory" %>
<%@ page import="com.google.appengine.api.datastore.DatastoreService" %>
<%@ page import="com.google.appengine.api.datastore.Entity" %>
<%@ page import="com.google.appengine.api.datastore.EntityNotFoundException" %>
<%@ page import="com.google.appengine.api.datastore.Key" %>
<%@ page import="com.google.appengine.api.datastore.KeyFactory" %>

<html>
  <body>

    <h1>Generate random data sets</h1>
    <p>Each dataset will be stored in JSON format and contain 300 variables and their values. Variables are named "VAR000".."VAR200". Values are random and lay between -50.0 and 50.0</p>   
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
	
  <%= fileIndex %> datasets currently in the database.

  <form action="/generate" method="get">
  	Choose how many random datasets to generate (max. 100): 
    <div><input type="text" name="number"/></div>
    <div><input type="submit" value="Generate..." /></div>
  </form>
  
  </body>
</html>