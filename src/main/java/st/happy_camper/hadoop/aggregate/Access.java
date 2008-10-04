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

import java.util.Date;

import org.apache.commons.lang.builder.CompareToBuilder;

/**
 * @author ueshin
 */
public class Access implements Comparable<Access> {

    private final String ip;

    private final String url;

    private final Date accessDate;

    /**
     * @param ip
     * @param url
     * @param accessDate
     */
    public Access(String ip, String url, Date accessDate) {
        this.ip = ip;
        this.url = url;
        this.accessDate = accessDate;
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

    /**
     * @return the accessDate
     */
    public Date getAccessDate() {
        return accessDate;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @Override
    public int compareTo(Access o) {
        return new CompareToBuilder().append(ip, o.getIp()).append(url,
                o.getUrl()).append(accessDate, o.getAccessDate())
                .toComparison();
    }

}
