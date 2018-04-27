/**
 * Name:       AppConfig
 * Purpose:    Load application properties file.
 * Author:     PNDA team
 *
 * Created:    07/04/2016
 */
/*
Copyright (c) 2016 Cisco and/or its affiliates.

This software is licensed to you under the terms of the Apache License, Version 2.0 (the "License").
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

The code, technical concepts, and all information contained herein, are the property of Cisco Technology, Inc.
and/or its affiliated entities, under various laws including copyright, international treaties, patent,
and/or contract. Any use of the material herein must be in accordance with the terms of the License.
All rights not expressly granted by the License are reserved.

Unless required by applicable law or agreed to separately in writing, software distributed under the
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
express or implied.
*/

package com.cisco.pnda.examples.flink;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;

public class AppConfig
{
    private static final Logger LOGGER = Logger.getLogger(AppConfig.class);

    private static Properties _properties;
    private static Object _lock = new Object();

    public static Properties loadProperties()
    {
        InputStream is = AppConfig.class.getClassLoader().getResourceAsStream("application.properties");

        synchronized (_lock)
        {
            if (_properties == null)
            {
                _properties = new Properties();
                try
                {
                    _properties.load(is);
                    LOGGER.info("Properties loaded");
                }
                catch (IOException e)
                {
                    LOGGER.info("Failed to load properties", e);
                    System.exit(1);
                }
            }
            return _properties;
        }
    }
}