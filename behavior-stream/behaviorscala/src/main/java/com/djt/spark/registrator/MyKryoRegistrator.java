package com.djt.spark.registrator;

import com.djt.model.SingleUserBehaviorRequestModel;
import com.djt.model.UserBehavorRequestModel;
import com.esotericsoftware.kryo.Kryo;
import org.apache.spark.serializer.KryoRegistrator;

public class MyKryoRegistrator implements KryoRegistrator
{
  @Override
  public void registerClasses(Kryo kryo)
  {
    kryo.register(UserBehavorRequestModel.class);
    kryo.register(SingleUserBehaviorRequestModel.class);
  }
}
