/**
  * Name:       StaticHelpers
  * Purpose:    Helper functions
  * Author:     PNDA team
  *
  * Created:    07/04/2016
  */

package com.cisco.pnda.model;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

public class StaticHelpers {
	public static String loadResourceFile(String path)
    {
        InputStream is = StaticHelpers.class.getClassLoader().getResourceAsStream(path);
        try
        {
            char[] buf = new char[2048];
            Reader r = new InputStreamReader(is, "UTF-8");
            StringBuilder s = new StringBuilder();
            while (true)
            {
                int n = r.read(buf);
                if (n < 0)
                    break;
                s.append(buf, 0, n);
            }
            return s.toString();
        }
        catch (Exception e)
        {
            return null;
        }
    }
}