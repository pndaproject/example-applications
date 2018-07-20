/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cisco.pnda.examples.flink.util;
import java.util.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * Provides the default data sets used for the WordCount example program.
 * The default data sets are used, if no parameters are given to the program.
 *
 */
public class WordCountData {
        // Added to increase execution time
        public static final int REP_COUNT = 1;

        public static final String[] WORDS = new String[] {
                "#test #test #test #test #test #test #test #test #test #test",
                "To be, or not to be,--that is the question:--",
                "#test #test #test #test #test #test #test #test #test #test",
                "Whether 'tis nobler in the mind to suffer",
                "#test #test #test #test #test #test #test #test #test #test",
                "The slings and arrows of outrageous fortune",
                "#test #test #test #test #test #test #test #test #test #test",
                "Or to take arms against a sea of troubles,",
                "#test #test #test #test #test #test #test #test #test #test",
                "And by opposing end them?--To die,--to sleep,--",
                "#test #test #test #test #test #test #test #test #test #test",
                "No more; and by a sleep to say we end",
                "#test #test #test #test #test #test #test #test #test #test",
                "The heartache, and the thousand natural shocks",
                "#test #test #test #test #test #test #test #test #test #test",
                "That flesh is heir to,--'tis a consummation",
                "#test #test #test #test #test #test #test #test #test #test",
                "Devoutly to be wish'd. To die,--to sleep;--",
                "#test #test #test #test #test #test #test #test #test #test",
                "To sleep! perchance to dream:--ay, there's the rub;",
                "#test #test #test #test #test #test #test #test #test #test",
                "For in that sleep of death what dreams may come,",
                "#test #test #test #test #test #test #test #test #test #test",
                "When we have shuffled off this mortal coil,",
                "#test #test #test #test #test #test #test #test #test #test",
                "Must give us pause: there's the respect",
                "#test #test #test #test #test #test #test #test #test #test",
                "That makes calamity of so long life;",
                "#test #test #test #test #test #test #test #test #test #test",
                "For who would bear the whips and scorns of time,",
                "#test #test #test #test #test #test #test #test #test #test",
                "The oppressor's wrong, the proud man's contumely,",
                "#test #test #test #test #test #test #test #test #test #test",
                "The pangs of despis'd love, the law's delay,",
                "#test #test #test #test #test #test #test #test #test #test",
                "The insolence of office, and the spurns",
                "#test #test #test #test #test #test #test #test #test #test",
                "That patient merit of the unworthy takes,",
                "#test #test #test #test #test #test #test #test #test #test",
                "When he himself might his quietus make",
                "#test #test #test #test #test #test #test #test #test #test",
                "With a bare bodkin? who would these fardels bear,",
                "#test #test #test #test #test #test #test #test #test #test",
                "To grunt and sweat under a weary life,",
                "#test #test #test #test #test #test #test #test #test #test",
                "But that the dread of something after death,--",
                "The undiscover'd country, from whose bourn",
                "#test #test #test #test #test #test #test #test #test #test",
                "No traveller returns,--puzzles the will,",
                "#test #test #test #test #test #test #test #test #test #test",
                "And makes us rather bear those ills we have",
                "#test #test #test #test #test #test #test #test #test #test",
                "Than fly to others that we know not of?",
                "#test #test #test #test #test #test #test #test #test #test",
                "Thus conscience does make cowards of us all;",
                "#test #test #test #test #test #test #test #test #test #test",
                "And thus the native hue of resolution",
                "#test #test #test #test #test #test #test #test #test #test",
                "Is sicklied o'er with the pale cast of thought;",
                "#test #test #test #test #test #test #test #test #test #test",
                "And enterprises of great pith and moment,",
                "#test #test #test #test #test #test #test #test #test #test",
                "With this regard, their currents turn awry,",
                "#test #test #test #test #test #test #test #test #test #test",
                "And lose the name of action.--Soft you now!",
                "#test #test #test #test #test #test #test #test #test #test",
                "The fair Ophelia!--Nymph, in thy orisons",
                "#test #test #test #test #test #test #test #test #test #test",
                "Be all my sins remember'd.",
                "#test #test #test #test #test #test #test #test #test #test"
        };

        public static DataSet<String> getDefaultTextLineDataSet(ExecutionEnvironment env) {

            // return env.fromElements(WORDS);
            List<String> list = new ArrayList<String>();
            for(int i=1;i<=REP_COUNT;i++) {
                    list.addAll(Arrays.asList(WORDS));
            }

            String[] bigWord = (String[]) list.toArray(new String[0]);
             return env.fromElements(bigWord);
        }
}
