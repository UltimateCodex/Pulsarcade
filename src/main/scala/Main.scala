import scala.io.Source
import scala.util.Random
import scala.io.StdIn._
import java.sql.DriverManager
import java.sql.Connection
import org.apache.spark.sql.SparkSession

object Main{
  def main(args: Array[String]):Unit = {
    sparkConnect()
    bar(10,400)
    title(250)
    menu()
  }

  //Spark
  def sparkConnect():Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    println("created spark session")
    spark.sql("CREATE TABLE IF NOT EXISTS pulsars(ID int, NAME varchar(16), P0 double, P1 double, DIST double, ZZ double, XX double, YY double, AGE double, BSURF double, EDOT double) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'");
    //spark.sql("LOAD DATA LOCAL INPATH 'Data.txt' INTO TABLE pulsars")
    spark.sql("SELECT * FROM pulsars").show()

    var X = spark.sql("SELECT XX FROM pulsars")
    var Y = spark.sql("SELECT YY FROM pulsars")
    var Z = spark.sql("SELECT ZZ FROM pulsars")
  }

  def bar(max:Int, delay:Int) :Unit = {
    for(i <- 0 to max){
      print("\r[" + "="*i + ">" + " "*(max-i) + "] ")
      Thread.sleep(delay)
    }
    println()
  }

  def title(delay:Int): Unit = {
    println("\t\t\t\t\t\t\t\t\t\t\t\t███████\\            ██\\                                                        ██\\                  ")
    Thread.sleep(delay)
    println("\t\t\t\t\t\t\t\t\t\t\t\t██  __██\\           ██ |                                                       ██ |                   ")
    Thread.sleep(delay)
    println("\t\t\t\t\t\t\t\t\t\t\t\t██ |  ██ |██\\   ██\\ ██ | ███████\\  ██████\\   ██████\\   ███████\\ ██████\\   ███████ | ██████\\    ")
    Thread.sleep(delay)
    println("\t\t\t\t\t\t\t\t\t\t\t\t███████  |██ |  ██ |██ |██  _____| \\____██\\ ██  __██\\ ██  _____|\\____██\\ ██  __██ |██  __██\\     ")
    Thread.sleep(delay)
    println("\t\t\t\t\t\t\t\t\t\t\t\t██  ____/ ██ |  ██ |██ |\\██████\\   ███████ |██ |  \\__|██ /      ███████ |██ /  ██ |████████ |       ")
    Thread.sleep(delay)
    println("\t\t\t\t\t\t\t\t\t\t\t\t██ |      ██ |  ██ |██ | \\____██\\ ██  __██ |██ |      ██ |     ██  __██ |██ |  ██ |██   ____|        ")
    Thread.sleep(delay)
    println("\t\t\t\t\t\t\t\t\t\t\t\t██ |      \\██████  |██ |███████  |\\███████ |██ |      \\███████\\███████ |\\███████ |\\███████\\     ")
    Thread.sleep(delay)
    println("\t\t\t\t\t\t\t\t\t\t\t\t\\__|       \\______/ \\__|\\_______/  \\_______|\\__|       \\_______|\\_______| \\_______| \\_______|")
    Thread.sleep(delay)
  }

  def menu():Unit = {
    Thread.sleep(1000)
    println("\t 1. New Game")
    println("\t 2. Load Game")
    println("\t 3. Tutorial")
    println("\t 4. Database")
    println("\t 5. Exit")
    val option = readLine(">")
    option match{
      case "1" => newGame()
      case "2" => loadGame()
      case "3" => tutorial()
      case "4" => database()
      case "5" => exit()
      case _ => menu()
    }
  }

  def actions():Unit = {
    Thread.sleep(1000)
    println("What action would you like to take?")
    println("\t 1. Status")
    println("\t 2. Move")
    println("\t 3. Refuel")
    println("\t 4. Do science!")
    println("\t 5. Star jump!!!")
    println("\t 6. Save game")
    println("\t 7. Menu")
    val option = readLine(">")
    option match{
      case "1" => status()
      case "2" => move()
      case "3" => refuel()
      case "4" => science()
      case "5" => jump()
      case "6" => saveGame()
      case "7" => menu()
      case _ => actions()
    }
  }

  var name:String    = ""
  var pass:String    = ""
  var xPos:Double    = 0.0
  var yPos:Double    = 0.0
  var zPos:Double    = 0.0
  var unit:Double    = 0.0
  var curFuel:Double = 0.0
  var maxFuel:Int    = 0
  var speed:Int      = 0
  var sciPts:Int     = 0

  def newGame():Unit = {
    var connection:Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/pulsardatabase", "root", "Science42")
    val gameID = connection.createStatement().executeQuery("SELECT ID FROM games")
    //spark.sql("SELECT ID FROM games")

    var ID = ""
    while (gameID.next()) {
      ID = (gameID.getInt("ID") + 1).toString
    }
    val nameNew = readLine("Name your game: ")
    val passNew = readLine("Password: ")

    connection.createStatement().execute("INSERT INTO games VALUES('" + ID + "','" + nameNew + "','" + passNew + "'," + "0.0, 8500.0, 0.0, 5.0, 40.0, 50, 100, 0)")
    name     = nameNew
    pass     = passNew
    xPos     = 0.0
    yPos     = 8500.0
    zPos     = 0.0
    unit     = 5.0
    curFuel  = 40.0
    maxFuel  = 50
    speed    = 100
    sciPts   = 0

    actions()
  }

  def loadGame():Unit = {
    var connection:Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/pulsardatabase", "root", "Science42")
    val resultSet = connection.createStatement().executeQuery("SELECT * FROM games")
    //spark.sql("SELECT * FROM games")

    println("+----------------+----------------+----------------+----------------+----------------+----------------+----------------+----------------+----------------+----------------+----------------+")
    println("|     Game ID    |      user      |      pass      |      xPos      |      yPos      |      zPos      |      unit      |    curFuel     |     maxFuel    |     speed      |     sciPts     |")
    println("+----------------+----------------+----------------+----------------+----------------+----------------+----------------+----------------+----------------+----------------+----------------+")
    while (resultSet.next()) {
      var result = ""
      for (i <- 1 to 11){
        result += " " + resultSet.getString(i) + " "*(15 - resultSet.getString(i).length) + "|"
      }
      println("|" + result)
    }
    println("+----------------+----------------+----------------+----------------+----------------+----------------+----------------+----------------+----------------+----------------+----------------+")

    val input = readLine("Which game to load? ")
    val gameID = connection.createStatement().executeQuery("SELECT ID FROM games")
    var break = false
    while (gameID.next()) {
      if(input == gameID.getInt("ID").toString) {
        val game = connection.createStatement().executeQuery("SELECT * FROM games WHERE ID = '" + input + "'")
        while (game.next()) {
          name    = game.getString("user")
          pass    = game.getString("pass")
          xPos    = game.getDouble("xPos")
          yPos    = game.getDouble("yPos")
          zPos    = game.getDouble("zPos")
          unit    = game.getDouble("unit")
          curFuel = game.getDouble("curFuel")
          maxFuel = game.getInt("maxFuel")
          speed   = game.getInt("speed")
          sciPts  = game.getInt("sciPts")
        }
        break = true
      }
    }
    if(break){
      actions()
    }
    else {
      println("Game ID does not exist...")
      menu()
    }
  }

  def saveGame():Unit = {
    var connection:Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/pulsardatabase", "root", "Science42")
    val resultSet = connection.createStatement().executeQuery("SELECT * FROM games")
    //spark.sql("

    println("+----------------+----------------+----------------+----------------+----------------+----------------+----------------+----------------+----------------+----------------+----------------+")
    println("|     Game ID    |      user      |      pass      |      xPos      |      yPos      |      zPos      |      unit      |    curFuel     |     maxFuel    |     speed      |     sciPts     |")
    println("+----------------+----------------+----------------+----------------+----------------+----------------+----------------+----------------+----------------+----------------+----------------+")
    while (resultSet.next()) {
      var result = ""
      for (i <- 1 to 11){
        result += " " + resultSet.getString(i) + " "*(15 - resultSet.getString(i).length) + "|"
      }
      println("|" + result)
    }
    println("+----------------+----------------+----------------+----------------+----------------+----------------+----------------+----------------+----------------+----------------+----------------+")

    val input = readLine("Which game to save over? ")
    val gameID = connection.createStatement().executeQuery("SELECT ID FROM games")
    var break = false
    while (gameID.next()) {
      if(input == gameID.getInt("ID").toString) {
        connection.createStatement().execute("UPDATE games SET user = '" + name + "', pass = '" + pass + "', xPos = " + xPos + ", yPos = " + yPos + ", zPos = " + zPos + ", unit = " + unit + ", curFuel = " + curFuel + ", maxFuel = " + maxFuel + ", speed = " + speed + ", sciPts = " + sciPts + " WHERE ID = '" + input + "'")
        break = true
      }
    }
    if(break){
      menu()
    }
    else {
      println("Game ID does not exist...")
      actions()
    }
  }

  def status():Unit = {
    var connection:Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/pulsardatabase", "root", "Science42")
    val resultSet = connection.createStatement().executeQuery("SELECT XX,YY FROM pulsars")
    //spark.sql("SELECT XX,YY FROM pulsars")

    var xGrid = Array[Int]()
    var yGrid = Array[Int]()
    while (resultSet.next()){
      xGrid :+= ((1000 * resultSet.getDouble("XX") - xPos)/unit).toInt
      yGrid :+= ((1000 * resultSet.getDouble("YY") - yPos)/unit).toInt
    }

    print("STATUS UPDATE:")
    println(screen(30,10,xGrid,yGrid))
    println("Current Position: ( " + xPos.toInt.toString + ", " + yPos.toInt.toString + ", " + zPos.toInt.toString + ")")
    println("Current Fuel: [" + "="*curFuel.toInt + "|" + "-"*(maxFuel-curFuel.toInt-1) + "]")
    println("Science Points: " + sciPts.toString)
    println()

    actions()
  }

  def move():Unit = {
    //need a try-catch here:

    val xInput = readLine("X distance to move? ").toDouble
    xPos += xInput
    val yInput = readLine("Y distance to move? ").toDouble
    yPos += yInput
    val zInput = readLine("Z distance to move? ").toDouble
    zPos += zInput

    val dist = Math.sqrt(xInput*xInput + yInput*yInput + zInput*zInput)

    curFuel -= dist/unit
    if(curFuel <= 0){
      println("You ran out of Fuel in deep space! GAME OVER!!!")
      menu()
    }

    println("In transit...")
    bar(dist.toInt,speed)

    if(xPos == 0 && yPos == 0 && xPos == 0){
      println("Congratulations! You've Won!!!")
      title(300)
      menu()
    }

    status()
  }

  def refuel():Unit = {
    var connection:Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/pulsardatabase", "root", "Science42")
    val resultSet = connection.createStatement().executeQuery("SELECT P0,XX,YY,ZZ FROM pulsars")
    //spark.sql("SELECT P0,XX,YY,ZZ FROM pulsars")

    var rate = 0.0
    var close = false
    while (resultSet.next()){
      if(Math.abs(xPos - 1000 * resultSet.getDouble("XX")) <= unit
      && Math.abs(yPos - 1000 * resultSet.getDouble("YY")) <= unit
      && Math.abs(zPos - 1000 * resultSet.getDouble("ZZ")) <= 1000*unit){
        rate = resultSet.getDouble("P0")
        close = true
      }
    }
    if(close){
      println("Fueling up!")
      curFuel = curFuel.toInt.toDouble
      while(curFuel < maxFuel){
        curFuel += 0.5
        print("\r " + "*"*((2*curFuel - 1).toInt%2) + " "*((2*curFuel).toInt%2) + " [" + "="*curFuel.toInt + "|" + "-"*(maxFuel-curFuel.toInt) + "]")
        Thread.sleep((500 * rate).toInt)
      }
      println()
    }
    else{
      println("Not close enough to Pulsar!")
    }

    actions()
  }

  def science():Unit = {
    var connection:Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/pulsardatabase", "root", "Science42")
    val resultSet = connection.createStatement().executeQuery("SELECT NAME,P0,XX,YY,ZZ,AGE,BSURF,EDOT FROM pulsars")
    //spark.sql("SELECT NAME,XX,YY,ZZ FROM pulsars")

    var name = ""
    var rate = 0.0
    var age = 0.0
    var bsurf = 0.0
    var edot = 0.0
    var close = false
    while (resultSet.next()){
      if  (Math.abs(xPos - 1000 * resultSet.getDouble("XX")) <= unit
        && Math.abs(yPos - 1000 * resultSet.getDouble("YY")) <= unit
        && Math.abs(zPos - 1000 * resultSet.getDouble("ZZ")) <= 1000*unit){
        name = resultSet.getString("NAME")
        rate = resultSet.getDouble("P0")
        age = resultSet.getDouble("AGE")
        bsurf = resultSet.getDouble("BSURF")
        edot = resultSet.getDouble("EDOT")
        close = true
      }
    }
    if(close){
      println("Gathering data...")
      for(t <- 0 to 50){
        print("\r{ " + "*"*(t%2) + " "*((t+1)%2) + " }")
        Thread.sleep((500 * rate).toInt)
      }
      println()
      println("DATA RECOVERED FROM PULSAR: " + name)
      println("Pulsar dated to be " + age.toInt.toString + " years old!")
      println("Its extreme magnetic fields are " + (bsurf/0.45).toInt.toString + " times that of Earth!")
      println("And it is " + (((edot/3.83E33)*100).toInt/100.0).toString + " times the brightness of the Sun!")

      var earn = 20 + Random.nextInt(80)
      sciPts += earn
      println("\n" + earn + " science points earned...")
    }
    else{
      println("Pulsar outside of sensor range!")
    }

    actions()
  }

  def jump():Unit = {
    var connection:Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/pulsardatabase", "root", "Science42")
    val resultSet = connection.createStatement().executeQuery("SELECT NAME,XX,YY,ZZ FROM pulsars")
    //spark.sql("SELECT NAME,XX,YY,ZZ FROM pulsars")

    var count = 1
    var jumpList = Array[String]()
    while (resultSet.next()) {
      if  (Math.abs(xPos - 1000 * resultSet.getDouble("XX")) <= 20 * unit
        && Math.abs(yPos - 1000 * resultSet.getDouble("YY")) <= 20 * unit
        && Math.abs(zPos - 1000 * resultSet.getDouble("ZZ")) <= 20 * unit) {
        //val dist = Math.sqrt(xInput*xInput + yInput*yInput + zInput*zInput)
        println(count + ": " + resultSet.getString("NAME"))
        jumpList :+= resultSet.getString("NAME")
        count += 1
      }
    }
    val option = readLine("Jump to which pulsar? ")
    if(option.toInt <= jumpList.length){
      curFuel = 0
      val jumpPos = connection.createStatement().executeQuery("SELECT XX,YY,ZZ FROM pulsars WHERE NAME = '" + jumpList(option.toInt - 1) + "'")
      while(jumpPos.next()){
        xPos = 1000 * jumpPos.getDouble("XX")
        yPos = 1000 * jumpPos.getDouble("YY")
        zPos = 1000 * jumpPos.getDouble("ZZ")
      }

      println("Jump successful!!!")
      status()
    }
    else{
      jump()
    }
  }

  def proximity():Unit = {

  }

  def screen(length:Int, height:Int, xGrid:Array[Int], yGrid:Array[Int]): String = {
    var result = ""
    for(i <- -height to height; j <- -length to length){
      if(j == -length){
        result += "\n"
      }
      if(i != -height  && i != height && j != -length && j != length){
        if(i == 0 && j == 0){
          result += "X"
        }
        else{
          var isStar = false
          for(p <- xGrid.indices){
            if(xGrid(p) == i && yGrid(p) == j){
              isStar = true
            }
          }
          if(isStar){
            result += "*"
          }
          else{
            result += " "
          }
        }
      }
      else{
        if((i == -height && (j == -length || j == length)) || (i == height && (j == -length || j == length))){
          result += "╬"
        }
        else{
          if(i == -height || i == height){
            result += "═"
          }
          if(j == -length || j == length){
            result += "║"
          }
        }
      }
    }
    return result
  }

  def typewriter(S:String):Unit = {
    for(s <- S){
      print(s)
      Thread.sleep(50)
    }
    Thread.sleep(2000)
    println()
  }

  def tutorial():Unit = {
    typewriter("Greetings space cadet! The united planetary admiralty has just equipped your ship with the newest in star-hopping technology! Your mission: adventure to the supermassive black hole in the center of our galaxy!")
    menu()
  }

  def database():Unit = {
    var connection:Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/pulsardatabase", "root", "Science42")
    val resultSet = connection.createStatement().executeQuery("SELECT * FROM pulsars")
    //spark.sql("SELECT * FROM pulsars")

    println("+----------------+----------------+----------------+----------------+----------------+----------------+----------------+----------------+----------------+----------------+----------------+")
    println("|       ID       |      Name      |     Period     |  Period change |    Distance    |   Z Position   |   X Position   |   Y Position   |       Age      | Magnetic Field |    Luminosity  |")
    println("+----------------+----------------+----------------+----------------+----------------+----------------+----------------+----------------+----------------+----------------+----------------+")
    while (resultSet.next()) {
      var result = ""
      for (i <- 1 to 11){
        result += " " + resultSet.getString(i) + " "*(15 - resultSet.getString(i).length) + "|"
      }
      println("|" + result)
    }
    println("+----------------+----------------+----------------+----------------+----------------+----------------+----------------+----------------+----------------+----------------+----------------+")

    val option = readLine("Would you like to update the database with data from the ATNF Pulsar Catalogue (Y/N)? ")
    if(option == "Y" | option == "y") {
      println("Database already up to date with https://www.atnf.csiro.au/research/pulsar/psrcat/!")
    }
    menu()
  }

  def exit():Unit = {
    var connection:Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/pulsardatabase", "root", "Science42")
    connection.close()
    println("Goodbye!")
  }
}





//multi-dim array
//var a = ofDim[Int](2,3)


//read data from file
/*
val file = Source.fromFile("Data.txt")
val rows = file.getLines().drop(2).toArray
val data = rows.map(_.split("\t"))
val pulnum = 2341

var xGrid = new Array[Int](pulnum)
var yGrid = new Array[Int](pulnum)
for(p <- 0 until pulnum){
  xGrid(p) = ((data(p)(6).toDouble - xPos)/unit).toInt
  yGrid(p) = ((data(p)(7).toDouble - yPos)/unit).toInt
}
*/

//Import file from URL: 
/*
import sys.process._
import java.net.URL
import java.io.File
import scala.language.postfixOps
val url = "https://www.atnf.csiro.au/research/pulsar/psrcat/proc_form.php?version=1.65&Name=Name&P0=P0&P1=P1&Dist=Dist&ZZ=ZZ&XX=XX&YY=YY&Age=Age&Bsurf=Bsurf&Edot=Edot&startUserDefined=true&c1_val=&c2_val=&c3_val=&c4_val=&sort_attr=Dist&sort_order=asc&condition=Dist+%3E%3D+0+%26%26+Dist+%3C+30+%26%26+P0+%3E+0+%26%26+P1+%3E+0&pulsar_names=&ephemeris=short&coords_unit=raj%2Fdecj&radius=&coords_1=&coords_2=&style=Long+with+last+digit+error&no_value=*&fsize=3&x_axis=XX&x_scale=linear&y_axis=YY&y_scale=linear&state=query&table_bottom.x=48&table_bottom.y=25"
new URL(url) #> new File("DataFromURL.txt") !!
//or
val src = scala.io.Source.fromURL(url)
val out = new java.io.FileWriter("DataFromURL.txt")
val html = src.mkString
*/







//figure out console
//fuel double
//encrypt pass
//Sparl -> SQL

//closest pulsar