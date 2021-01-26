package com.atguigu.gmall0921.realtime.bean

/**
 * @author mytstart
 * @create 20:27-01-26
 */
object DauInfo {

  case class DauInfo(
                      mid:String,
                      uid:String,
                      ar:String,
                      ch:String,
                      vc:String,
                      var dt:String,
                      var hr:String,
                      ts:Long) {
  }
}
