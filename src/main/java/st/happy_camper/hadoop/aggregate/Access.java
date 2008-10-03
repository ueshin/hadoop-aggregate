/*
 * Copyright 2008 Happy-Camper Street.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package st.happy_camper.hadoop.aggregate;

/**
 * @author ueshin
 */
public class Access implements Comparable<Access> {

    private final String ip;

    private final String url;

    /**
     * @param ip
     * @param url
     */
    public Access(String ip, String url) {
        this.ip = ip;
        this.url = url;
    }

    /**
     * @return the ip
     */
    public String getIp() {
        return ip;
    }

    /**
     * @return the url
     */
    public String getUrl() {
        return url;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @Override
    public int compareTo(Access o) {
        if(o == null) {
            throw new NullPointerException();
        }
        else {
            int ip = getIp().compareTo(o.getIp());
            if(ip != 0) {
                return ip;
            }
            else {
                return getUrl().compareTo(o.getUrl());
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return String.format(" [%s, %s]", getIp(), getUrl());
    }

}
