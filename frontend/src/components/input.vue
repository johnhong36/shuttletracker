<head>    
    <meta charset="utf-8">
    <title>Registration</title>
</head>

<body>
    
  <h2>Notification Registration</h2><br>
    
  <form onsubmit="return add(this)">

    <label for ="RCS">RCS ID:</label>
      <input type="text" value="" name="RCS"><br>
        
    <label for = "stop">Target Stop:</label>
      <select  name="stop">
        <option value="stop1" >stop1</option>
        <option value="stop2" >stop2</option>
        <option value="stop3" >stop3</option>
        <option value="stop4" >stop4</option>
      </select><br>
      
    <label for = "route">Shuttle Route:</label>
      <select name="route">
        <option value="East">East</option>
        <option value="West">West</option>
      </select><br>
        
    <label for = "phone">Phone Number:</label>
      <input type="text" value="" name ="phone"><br>
        
    
    <label for ="carrier">Carrier Type:</label>
      <input type="text" value="" name="carrier"><br>
        
    <input type="submit" value="Submit"><br>

  </form>

</body>

<script type>
  function add(formObj)
  {

    var data = 
    {
      RCS: formObj.RCS.value,
      stop: formObj.stop.value,
      route: formObj.route.value,
      phone: formObj.phone.value,
      carrier: formObj.carrier.value,
    }

    window.localStorage.setItem(formObj.RCS.value, JSON.stringify(data));
    console.log("Local Storage");
    for (i = 0; i < localStorage.length; i++)   
    {
      console.log(localStorage.key(i) + " = \n" + localStorage.getItem(localStorage.key(i)) );
    }

    //window.localStorage.clear();

    return true;
  }

</script>


