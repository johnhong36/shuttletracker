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
      <option value="Student Union" >Student Union</option>
      <option value="Tibbits Ave" >Tibbits Ave</option>
      <option value="BarH" >BarH</option>
      <option value="Colonie Apartments" >Colonie Apartments</option>
      <option value="Brinsmade Terrace" >Brinsmade Terrace</option>
      <option value="Beman Lane" >Beman Lane</option>
      <option value="Sunset Terrace" >Sunset Terrace</option>
      <option value="13th and Peoples Ave." >13th and Peoples Ave.</option>
      <option value="9th and Sage" >9th and Sage</option>
      <option value="West Hall" >West Hall</option>
      <option value="Troy Building Crosswalk" >Troy Building Crosswalk</option>
      <option value="Sage" >Sage</option>
      <option value="Blitman Residence Commons" >Blitman Residence Commons</option>
      <option value="6th Ave. and City Station" >6th Ave. and City Station</option>
      <option value="Polytechnic Residence Commons" >Polytechnic Residence Commons</option>
      <option value="15th and College Ave." >15th and College Ave.</option>
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

