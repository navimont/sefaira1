<!doctype html>
<html>
  <head>
		<meta charset="utf-8"/>
    <meta http-equiv="content-type" content="text/html;charset=utf-8">
    <meta name="description" content=""/>

    <!-- Stylesheets -->
	<link rel="stylesheet" media="all" href="/Sefaira.css"/>
    <script src="http://ajax.googleapis.com/ajax/libs/jquery/1.5/jquery.min.js"></script>
	<script src="/highcharts.js" type="text/javascript"></script>
    <script src="/lookup.js"></script>
    <title>Variable browser</title>
  </head>
  
<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<%@ page import="com.google.appengine.api.datastore.DatastoreServiceFactory" %>
<%@ page import="com.google.appengine.api.datastore.DatastoreService" %>
<%@ page import="com.google.appengine.api.datastore.Entity" %>
<%@ page import="com.google.appengine.api.datastore.EntityNotFoundException" %>
<%@ page import="com.google.appengine.api.datastore.Key" %>
<%@ page import="com.google.appengine.api.datastore.Text" %>
<%@ page import="com.google.appengine.api.datastore.KeyFactory" %>
<%@ page import="com.google.gson.Gson" %>
<%@ page import="com.google.gson.reflect.TypeToken" %>
<%@ page import="java.lang.reflect.Type" %>
<%@ page import="java.util.Map" %>
<%@ page import="java.util.List" %>
<%@ page import="java.util.LinkedList" %>
<%@ page import="java.util.Collections" %>

  <body>
      <script type="text/javascript">
          $(document).ready(function() {
          	on_load();
          });
      </script>


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
	
	// retrieve last object from database to use it's content to fill the variable name selector
	List<String> vars;
	if (fileIndex > 0) {
		Key datakey = KeyFactory.createKey(counterKey, "jsondata", fileIndex);
		try {
			Text textdata = (Text) datastore.get(datakey).getProperty("data");
	    	Gson gson = new Gson();
	    	// needed to parse json data
	    	Type mapType = new TypeToken<Map<String,Float>>() {}.getType();
	    	Map<String,Float> varmap = gson.fromJson(textdata.getValue(), mapType);
	    	vars = new LinkedList<String>(varmap.keySet());
		} catch (EntityNotFoundException e) {
			vars = new LinkedList<String>();
		}
		Collections.sort(vars);
   	} else {
   		vars = new LinkedList<String>();
   		vars.add("<no data>");
   	}
%>

  <form action="/lookup" method="get">
	
	  <h2>Specify variable names</h2>  
	  
	    <div>
	    	Search for variable <select name="var1" size="1">
<%
	for (String varname : vars) {
%>	    	
    <option <% if (varname.equals(request.getParameter("var1"))) { %> selected <% } %> > <%= varname %> </option>
<%
 }
%>
	    	</select>
	    	in range from <input type="text" name="var1_min" value="<%= request.getParameter("var1_min") %>" onkeypress="delayed_lookup(event);">
	    	to <input type="text" name="var1_max" value="<%= request.getParameter("var1_max") %>" onkeypress="delayed_lookup(event);">
	    </div>
	    
	    <div>
	    	Search for variable <select name="var2" size="1" value="">
<%
	for (String varname : vars) {
%>	    	
    <option <% if (varname.equals(request.getParameter("var2"))) { %> selected <% } %> > <%= varname %> </option>
<%
 }
%>
	    	</select>
	    	in range from <input type="text" name="var2_min" value="<%= request.getParameter("var2_min") %>" onkeypress="delayed_lookup(event);">
	    	to <input type="text" name="var2_max" value="<%= request.getParameter("var2_max") %>" onkeypress="delayed_lookup(event);">
	    </div>
	    
	 <p>Search through <%= fileIndex %> datasets using 
	  	<select name="ntasks" size="1">

<%
	for (Integer ti=1; ti<=10; ti++) {
%>	 
    <option value="<%= ti %>" <% if (ti.toString().equals(request.getParameter("ntasks"))) { %>selected="selected"<% } %>> <%= ti %> </option>
<%
 }
%>
	  	</select>
	  parallel tasks.</p>
	  
	    <div><input type="submit" value="Start search" /></div>
  </form>
    
  <div id="chart-container" style="width: 700px; height: 400px">
  </div>

  <div id="task-list">
  <p>We are looking for your data...</p>
  </div>

  <p>Click here to <a href="/generate">generate more data.</a></p>
  <div id="vardata-link"></div>

  </body>
</html>