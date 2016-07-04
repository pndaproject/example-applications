/**
  * Name:       StaticHelpers
  * Purpose:    Helper functions
  * Author:     PNDA team
  *
  * Created:    07/04/2016
  */

/*
Copyright (c) 2016 Cisco and/or its affiliates.
 
This software is licensed to you under the terms of the Apache License, Version 2.0 (the "License").  You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 
The code, technical concepts, and all information contained herein, are the property of Cisco Technology, Inc. and/or its affiliated entities, under various laws including copyright, international treaties, patent, and/or contract. Any use of the material herein must be in accordance with the terms of the License. All rights not expressly granted by the License are reserved.
 
Unless required by applicable law or agreed to separately in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*/

package com.cisco.pnda.examples.spark.model;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

public class StaticHelpers {
    public static String loadResourceFile(String path) {
        InputStream inputStream = StaticHelpers.class.getClassLoader().getResourceAsStream(path);
        try
        {
            char[] chunk = new char[2048];
            Reader reader = new InputStreamReader(inputStream, "UTF-8");
            StringBuilder result = new StringBuilder();
            int charsRead = reader.read(chunk);
            while (charsRead > 0)
            {
                result.append(chunk, 0, charsRead);
                charsRead = reader.read(chunk);
            }
            return result.toString();
        }
        catch (Exception e)
        {
            return null;
        }
    }
}