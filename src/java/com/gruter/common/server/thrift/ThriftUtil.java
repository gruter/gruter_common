package com.gruter.common.server.thrift;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

public class ThriftUtil
{
	public static Object convertValueObject(Object sourceObject, Class<?> targetObjectClass) throws Exception
	{
		Object targetObject = targetObjectClass.newInstance ();
		
		Field[] fields = sourceObject.getClass ().getFields ();
		
		for(Field eachField: fields) 
		{
			Field targetField = targetObjectClass.getField (eachField.getName ()); 
			if(targetField != null) 
			{
				String methodName = eachField.getName ().substring (0, 1).toUpperCase () + eachField.getName ().substring (1);
				String getterName;
				
				if(eachField.getType ().equals (Boolean.TYPE)) 
				{
					getterName = "is" + methodName;
				} 
				else 
				{
					getterName = "get" + methodName;
				}
				
				try 
				{
					Method getMethod = sourceObject.getClass().getMethod (getterName);
					
					if(getMethod == null) 
					{
						continue;
					}
					String setterName = "set" + methodName;
					Method setMethod = targetObjectClass.getMethod (setterName, getMethod.getReturnType ());
					if(setMethod == null) 
					{
						continue;
					}
					setMethod.invoke (targetObject, getMethod.invoke (sourceObject));
				} 
				catch (NoSuchMethodException e) 
				{
					continue;
				}
			}
		}
		
		return targetObject;
	}
}
