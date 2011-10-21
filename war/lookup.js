/*
 * Code handles the background polling of results for the variable lookup.
 *  
 * It sends a /lookup request 0.5 seconds after being triggered and waits for a reply
 * If a reply arrives, it displays the results from the result data set
 * and after 0.5 seconds polls again for the next update.
 * It finishes, when the server process has finished or if no data is returned
 * by the server process.
 * 
 * Stefan Wehner (2011) 
 */

var vardata_req = new XMLHttpRequest();
var chart;
var start;
// the data to be rendered
var series;

function render () {
    // draw chart
	var var1 = {"text": $('select[name="var1"]').val()};
	var var2 = {"text": $('select[name="var2"]').val()};
	var var1_min = $('input[name="var1_min"]').val();
	var var1_max = $('input[name="var1_max"]').val();
	var var2_min = $('input[name="var2_min"]').val();
	var var2_max = $('input[name="var2_max"]').val();
    chart = new Highcharts.Chart({
        chart: {
           renderTo: 'chart-container',
           defaultSeriesType: 'scatter',
           zoomType: 'xy',
           width: 760,
           height: 400
        },
        title: {
           text: 'Scatter plot of filtered data'
        },
        xAxis: {
        	title: var1,
        	min: var1_min,
        	max: var1_max
        },
        yAxis: {
        	title: var2,
        	min: var2_min,
        	max: var2_max
        },
    	series: series
     });	
}

//callback for search result vardata on server
function vardata_reply() {
    if (vardata_req.readyState == 4) {
        if (vardata_req.status == 200) {
            var reply = JSON.parse(vardata_req.responseText);
            // the reply content is a map with three entries:
            var tasks = reply['tasks'];
            // print task status on page: <task name> <task status> <task run time>
            taskdata = "<table>";
            for (var task in tasks) {
            	taskdata += "<tr><td>Task: "+tasks[task]['name']+"</td>";
            	taskdata += "<td>"+tasks[task]['status']+"</td>";
            	taskdata += "<td>"+tasks[task]['time']+"</td></tr>";
            	if (tasks[task]['status'] == "RUNNING") {
            		running = true;
            	}
            }
            var elapsed = new Date().getTime() - start;
            taskdata += "<tr colspan=\"3\">Browser time: "+ elapsed/1000 +" seconds since request started.</tr>";
            taskdata += "</table>";
            $('#task-list').html(taskdata);
            series = reply['series'];
            render();
            // enqueue call again if tasks haven't finished
            if (reply['active']) {
            	delayed_lookup();
            } else {
                $('body').css('cursor', 'auto');
            }
        }
    }
}

// retrieves data and running processes for the ongoing background search on the server 
function vardata_request() {
	$('body').css('cursor', 'wait');
	var var1 = $('select[name="var1"]').val();
	var var2 = $('select[name="var2"]').val();
	var var1_min = $('input[name="var1_min"]').val();
	var var1_max = $('input[name="var1_max"]').val();
	var var2_min = $('input[name="var2_min"]').val();
	var var2_max = $('input[name="var2_max"]').val();
    var url = "/vardata?var1="+var1+"&var2="+var2+"&var1_min="+var1_min+"&var1_max="+var1_max+"&var2_min="+var2_min+"&var2_max="+var2_max;
    $('#vardata-link').html("<p>Click here to launch a <a href=\""+url+"\">direct REST Api call.</a></p>");
    vardata_req.open("GET", url, true);
    vardata_req.onreadystatechange = vardata_reply;
    vardata_req.send(null);
}


var lookup_timeout;
// this delays vardatas a little
function delayed_lookup(e) {
	// clear existing timeout (if exists)
	clearTimeout(lookup_timeout);
	// set new timeout
	lookup_timeout = setTimeout("vardata_request();", 500);
}

function render_on_return_key(event) {
    if (event.keyCode == 13) {
        render();
    }
}

// send a get request to start a new search
function search_request() {
	$('#search-form').submit();
    // form.submit();
}


function on_load() {
  	// take start time and send request for results
	start = new Date().getTime();
  	vardata_request();
}

