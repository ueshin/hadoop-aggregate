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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 * @author ueshin
 */
public class AccessWritable implements WritableComparable<AccessWritable> {

    private Access access;

    /**
     * 
     */
    public AccessWritable() {
    }

    /**
     * @param access
     */
    public AccessWritable(Access access) {
        this.access = access;
    }

    /**
     * @return the access
     */
    public Access getAccess() {
        return access;
    }

    /**
     * @param access
     *            the access to set
     */
    public void setAccess(Access access) {
        this.access = access;
    }

    /*
     * (non-Javadoc)
     * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
     */
    public void readFields(DataInput in) throws IOException {
        access = new Access(WritableUtils.readString(in), WritableUtils
                .readString(in), new Date(WritableUtils.readVLong(in)));
    }

    /*
     * (non-Javadoc)
     * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
     */
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeString(out, access.getIp());
        WritableUtils.writeString(out, access.getUrl());
        WritableUtils.writeVLong(out, access.getAccessDate().getTime());
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @Override
    public int compareTo(AccessWritable o) {
        return access.compareTo(o.getAccess());
    }

}
