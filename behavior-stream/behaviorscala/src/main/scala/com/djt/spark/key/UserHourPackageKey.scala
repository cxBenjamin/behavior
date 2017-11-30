package com.djt.spark.key

class UserHourPackageKey(val userId:String, val hour:String, var packageName:String) extends Ordered[UserHourPackageKey] with Serializable {
  override def compare(that: UserHourPackageKey): Int = {
    if (this.userId.compareTo(that.userId)==0) {
      if (this.hour.compareTo(that.hour) == 0) {
        return this.packageName.compareTo(that.packageName)
      } else {
        return this.hour.compareTo(that.hour)
      }
    } else {
      return this.userId.compareTo(that.userId)
    }
  }
}
