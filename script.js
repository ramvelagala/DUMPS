

$(function(){
    var table_data = [];
    $.getJSON('result.json', function(data) {
        $.each(data.result, function(i, f) { //for each loop
//            console.log(f.table_name)
            var tblRow = "<tr></tr>"

//            console.log(i, f)

                // Domain Metrics start
                if(f.rows != undefined)
                {
                    console.log(f.rows)
                    if(f.rows != 0)
                        {
                            tblRow+="<tr><th>Total Rows</th><th>Nulls</th></tr>"
                            tblRow+="<tr>"
                            tblRow+="<td>" + f.rows + "</td>"
                            if(f.nulls != null)
                             {
                                tblRow+="<td>" + f.nulls + "</td>"
                              }

                            tblRow+="</tr>+"


                            tblRow+="<tr><th>Cardinality</th><th>Uniqueness %</th></tr>"
                            tblRow+="<tr>"

                            if(f.cardinality != null)
                             {
                                tblRow+="<td>" + f.cardinality + "</td>"
                              }
                              if(f.uniqueness != null)
                             {
                                tblRow+="<td>" + f.uniqueness + "</td>"
                              }

                            tblRow+="</tr>+"
                        }
                    else
                        {
                        tblRow+="<tr>" + "<td> No of Rows - " + f.rows + "</td>" + "</tr>+"
                        }
                   }

                   // Domain Metrics end

                   // Range Metrics Start

                   if(f.mean != undefined)
                   {

                   var rmTbl = "</br><h4><center><b>Range Metrics</b></center></h4>"
                   rmTbl += "<table class='table' border = '1'>"

                         rmTbl+="<tr><th >Mean</th><th colspan='8'>Standard Deviation</th></tr>"
                            rmTbl+="<tr>"

                            if(f.mean != null)
                             {
                                rmTbl+="<td>" + f.mean + "</td>"
                              }
                              if(f.standard_deviation != null)
                             {
                                rmTbl+="<td colspan='8'>" +f.standard_deviation+ "</td>"
                              }

                        rmTbl+="</tr>"


                         rmTbl+="<tr><th>Lowest Value </th><th>Median</th><th>Highest Value</th></tr>"
                            rmTbl+="<tr>"

                            if(f.minimum != null)
                             {
                                rmTbl+="<td>" + f.minimum + "</td>"
                              }
                               if(f.median != null)
                             {
                                rmTbl+="<td>" + f.median + "</td>"
                              }
                              if(f.maximum != null)
                             {
                                rmTbl+="<td>" + f.maximum + "</td>"
                              }

                        rmTbl+="</tr>"

                   rmTbl += "</table>"
                   }


                   // Range Metrics  End




                   //Range Matrix Start




                   //Range Matrix End

                   //Value Matrix Start

                     if(f.value_metric != undefined)
                    {

                            var tblRow2 ="<h4><b>Most Common Values</b></h4>"
                            tblRow2+="<table class='table' > "
                            tblRow2 += "<tr><th>Value</th>"
                            tblRow2 += "<th>Count</th>"
                            tblRow2 += "<th>Coverage %</th></tr>"
                            $.each(f.value_metric, function(ind1, value) {

                                tblRow2 += "<tr>"
                                    tblRow2 += "<td>" + value.top_value + "</td>"
                                    tblRow2 += "<td>" + value.top_val_count + "</td>"
                                    tblRow2 += "<td>" + value.top_coverage + "</td>"
                               tblRow2 += "</tr>"

                                });
                                tblRow2 += "</table>"
                    }

                     if(f.value_metric_2 != undefined)
                    {
                            var tblRow2 = "<h4><b>Least Common Values</b></h4><table class='table'> "
                            tblRow2 += "<tr><th>Value</th>"
                            tblRow2 += "<th>Count</th>"
                            tblRow2 += "<th>Coverage %</th></tr>"
                            $.each(f.value_metric_2, function(ind1, value) {

                                tblRow2 += "<tr>"
                                    tblRow2 += "<td>" + value.bot_value + "</td>"
                                    tblRow2 += "<td>" + value.bot_val_count + "</td>"
                                    tblRow2 += "<td>" + value.bot_coverage + "</td>"
                               tblRow2 += "</tr>"

                                });
                                tblRow2 += "</table>"
                    }

                   //Value Matrix End

                   // Focus Value Start

//
//                    console.log(f.focus_metric)
//                    console.log("focus metric object")

                    if(f.focus_metric != undefined)
                    {
                            var tblRow1 = "<h4><b>Focus Metrics</b></h4><table class='table'> "
                            tblRow1 += "<th>Value</th>"
                            tblRow1 += "<th>Count</th>"
                            tblRow1 += "<th>Coverage %</th>"

                            $.each(f.focus_metric, function(ind1, value) {

                            tblRow1 += "<tr>"
                                    tblRow1 += "<td>" + value.focus_value + "</td>"
                                    tblRow1 += "<td>" + value.count + "</td>"
                                    tblRow1 += "<td>" + value.coverage + "</td>"
                               tblRow1 += "</tr>"

                                });
                                tblRow1 += "</table>"
                    }


                   // Focus Value End



            // appending json data to table body tag
             $(tblRow).appendTo("#userdata tbody");
             $(rmTbl).appendTo("#range_metrics");
             $(tblRow1).appendTo("#metrics_data");
             $(tblRow2).appendTo("#metrics_data");

        }); //for loop end



    }); //getjson close
});
