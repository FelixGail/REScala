package de.tuda.stg

import android.content.Context
import android.util.Log
import android.hardware.SensorManager
import android.os.Bundle
import android.support.v7.app.AppCompatActivity
import java.io.IOException
import reandroidthings._
import reandroidthings.driver.bmx280.Bmx280SensorDriver

class MainActivity extends AppCompatActivity {
  // allows accessing `.value` on TR.resource.constants
  implicit val context = this

  var sensorManager: SensorManager = null
  var reSensorManager: ReSensorManager = null
  private var mEnvironmentalSensorDriver: Bmx280SensorDriver = null
  //  private var mLastPressure = .0
  //  private var mLastTemperature = .0


  override def onCreate(savedInstanceState: Bundle): Unit = {
    super.onCreate(savedInstanceState)


    // type ascription is required due to SCL-10491
    //val vh: TypedViewHolder.main = TypedViewHolder.setContentView(this, TR.layout.main)
    //    vh.text.setText(s"Hello world, from ${TR.string.app_name.value}")
    //    vh.image.getDrawable match {
    //      case a: Animatable => a.start()
    //      case _ => // not animatable
    //    }


    // get SensorService and cast it to SensorManager
    sensorManager = getSystemService(Context.SENSOR_SERVICE) match {
      case sm: SensorManager => sm
      case _ => throw new ClassCastException
    }
    reSensorManager = ReSensorManager.wrap(sensorManager)

//    val deviceSensors = reSensorManager.getSensorList(ReSensor.TYPE_ALL)
//    Log.d("Barometer4Android", deviceSensors.toString)
//    Log.d("Barometer4Android", "isDynamicSensorDiscoverySupported: " + reSensorManager.isDynamicSensorDiscoverySupported.toString)


    try {
      mEnvironmentalSensorDriver = new Bmx280SensorDriver("I2C1")
      reSensorManager.registerDynamicSensorCallback(mDynamicSensorCallback)
      mEnvironmentalSensorDriver.registerTemperatureSensor
      mEnvironmentalSensorDriver.registerPressureSensor
      Log.d("Barometer4Android", "Initialized I2C BMP280")
    } catch {
      case e: IOException =>
        throw new RuntimeException("Error initializing BMP280", e)
    }
  }


  // --------------- Dynamic ---------------
  private val mDynamicSensorCallback = new ReSensorManager.DynamicSensorCallback() {
    override def onDynamicSensorConnected(sensor: ReSensor): Unit = {
      Log.d("Barometer4Android", "Hi")
      if (sensor.`type` == ReSensor.TYPE_AMBIENT_TEMPERATURE) { // Our sensor is connected. Start receiving temperature data.
        reSensorManager.registerListener(mTemperatureListener, sensor, ReSensorManager.SensorDelayNormal)
      }
      else if (sensor.`type` == ReSensor.TYPE_PRESSURE) { // Our sensor is connected. Start receiving pressure data.
        reSensorManager.registerListener(mPressureListener, sensor, ReSensorManager.SensorDelayNormal)
      }
    }

    override def onDynamicSensorDisconnected(sensor: ReSensor): Unit = {
      super.onDynamicSensorDisconnected(sensor)
    }
  }

  // Callback when SensorManager delivers temperature data.
  private val mTemperatureListener = new ReSensorEventListener() {
    override def onSensorChanged(event: ReSensorEvent): Unit = {
      Log.d("Barometer4Android", "onSensorChanged - temperature " + event.values(0))
      //      mLastTemperature = event.values(0)
      //      Log.d(TAG, "sensor changed: " + mLastTemperature)
    }

    override def onAccuracyChanged(sensor: ReSensor, accuracy: Int): Unit = {
      Log.d("Barometer4Android", "onAccuracyChanged - temperature " + accuracy)
    }
  }

  // Callback when SensorManager delivers pressure data.
  private val mPressureListener = new ReSensorEventListener() {
    override def onSensorChanged(event: ReSensorEvent): Unit = {
      Log.d("Barometer4Android", "onSensorChanged - pressure " + event.values(0))
      //      mLastPressure = event.values(0)
      //      Log.d(TAG, "sensor changed: " + mLastPressure)
      //      if (mDisplayMode eq DisplayMode.PRESSURE) updateDisplay(mLastPressure)
      //      updateBarometer(mLastPressure)
    }

    override def onAccuracyChanged(sensor: ReSensor, accuracy: Int): Unit = {
      Log.d("Barometer4Android", "onAccuracyChanged - pressure " + accuracy)
    }
  }
}
