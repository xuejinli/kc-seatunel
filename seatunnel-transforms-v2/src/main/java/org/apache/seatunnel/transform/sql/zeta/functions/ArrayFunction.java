/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.seatunnel.transform.sql.zeta.functions;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.utils.SeaTunnelException;

import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

public class ArrayFunction {

    public static Object[] array(List<Object> args) {
        if (args == null || args.isEmpty()) {
            return new Object[0];
        }
        Class<?> arrayType = getClassType(args);

        Object[] result = (Object[]) java.lang.reflect.Array.newInstance(arrayType, args.size());
        for (int i = 0; i < args.size(); i++) {
            result[i] = convertToType(args.get(i), arrayType);
        }

        return result;
    }

    public static ArrayType castArrayTypeMapping(Function function) {
        return castArrayTypeMapping(getFunctionArgs(function));
    }

    public static ArrayType castArrayTypeMapping(List<Object> args) {
        if (args == null || args.isEmpty()) {
            return ArrayType.STRING_ARRAY_TYPE;
        }

        Class<?> arrayType = getClassType(args);
        SeaTunnelDataType<?> seaTunnelDataType = getSeaTunnelDataType(arrayType);

        return new ArrayType(
                Array.newInstance(arrayType, args.size()).getClass(), seaTunnelDataType);
    }

    private static SeaTunnelDataType getSeaTunnelDataType(Class<?> clazz) {
        String className = clazz.getSimpleName();
        switch (className) {
            case "Integer":
                return BasicType.INT_TYPE;
            case "Double":
                return BasicType.DOUBLE_TYPE;
            case "Boolean":
                return BasicType.BOOLEAN_TYPE;
            case "Long":
                return BasicType.LONG_TYPE;
            case "float":
                return BasicType.FLOAT_TYPE;
            case "short":
                return BasicType.SHORT_TYPE;
            default:
                return BasicType.STRING_TYPE;
        }
    }

    private static Class<?> getArrayType(Class<?> type1, Class<?> type2) {
        if (type1.isAssignableFrom(type2)) {
            return type1;
        }
        if (type2.isAssignableFrom(type1)) {
            return type2;
        }
        if (isNumericType(type1) && isNumericType(type2)) {
            return getNumericCommonType(type1, type2);
        }
        return String.class;
    }

    private static boolean isNumericType(Class<?> type) {
        return type == Short.class
                || type == Integer.class
                || type == Long.class
                || type == Float.class
                || type == Double.class;
    }

    private static Class<?> getNumericCommonType(Class<?> type1, Class<?> type2) {
        if (type1 == Double.class || type2 == Double.class) {
            return Double.class;
        }
        if (type1 == Float.class || type2 == Float.class) {
            return Float.class;
        }
        if (type1 == Long.class || type2 == Long.class) {
            return Long.class;
        }
        if (type1 == Integer.class || type2 == Integer.class) {
            return Integer.class;
        }
        if (type1 == Short.class || type2 == Short.class) {
            return Short.class;
        }
        return String.class;
    }

    private static Class<?> getClassType(List<Object> args) {
        Class<?> arrayType = null;
        for (Object obj : args) {
            if (obj == null) {
                continue;
            }
            if (arrayType == null) {
                arrayType = obj.getClass();
            } else {
                arrayType = getArrayType(arrayType, obj.getClass());
            }
        }
        return arrayType == null ? String.class : arrayType;
    }

    private static List<Object> getFunctionArgs(Function function) {
        ExpressionList<Expression> expressionList =
                (ExpressionList<Expression>) function.getParameters();
        List<Object> functionArgs = new ArrayList<>();
        if (expressionList != null) {
            for (Expression expression : expressionList.getExpressions()) {
                if (expression instanceof NullValue) {
                    functionArgs.add(null);
                    continue;
                }
                if (expression instanceof DoubleValue) {
                    functionArgs.add(((DoubleValue) expression).getValue());
                    continue;
                }
                if (expression instanceof LongValue) {
                    long longVal = ((LongValue) expression).getValue();
                    if (longVal <= Integer.MAX_VALUE && longVal >= Integer.MIN_VALUE) {
                        functionArgs.add((int) longVal);
                    } else {
                        functionArgs.add(longVal);
                    }
                    continue;
                }
                if (expression instanceof StringValue) {
                    functionArgs.add(((StringValue) expression).getValue());
                    continue;
                }
                throw new SeaTunnelException("unSupport expression： " + expression.toString());
            }
        }
        return functionArgs;
    }

    private static Object convertToType(Object obj, Class<?> targetType) {
        if (obj == null || targetType.isInstance(obj)) {
            return obj;
        }

        if (targetType == Double.class) {
            return ((Number) obj).doubleValue();
        }
        if (targetType == Float.class) {
            return ((Number) obj).floatValue();
        }
        if (targetType == Long.class) {
            return ((Number) obj).longValue();
        }
        if (targetType == Integer.class) {
            return ((Number) obj).intValue();
        }
        if (targetType == Short.class) {
            return ((Number) obj).shortValue();
        }
        if (targetType == Byte.class) {
            return ((Number) obj).byteValue();
        }
        if (targetType == String.class) {
            return obj.toString();
        }

        throw new SeaTunnelException("Cannot convert " + obj.getClass() + " to " + targetType);
    }
}
