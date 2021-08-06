package org.greenplum.pxf.plugins.hdfs;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.InvalidInputException;
import org.apache.hadoop.mapred.JobConf;
import org.greenplum.pxf.api.model.BaseFragmenter;
import org.greenplum.pxf.api.model.ConfigurationFactory;
import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.api.model.FragmentStats;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.Utilities;
import org.greenplum.pxf.plugins.hdfs.utilities.HdfsUtilities;
import org.greenplum.pxf.plugins.hdfs.utilities.PxfInputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Fragmenter class for HDFS data resources.
 * <p>
 * Given an HDFS data source (a file, directory, or wild card pattern) divide
 * the data into fragments and return a list of them along with a list of
 * host:port locations for each.
 */
public class HdfsDataFragmenter extends BaseFragmenter {

    protected static final String IGNORE_MISSING_PATH_OPTION = "IGNORE_MISSING_PATH";
    private static final int MAX_BACKOFF = 1000;

    protected JobConf jobConf;
    protected HcfsType hcfsType;

    public HdfsDataFragmenter() {
    }

    HdfsDataFragmenter(ConfigurationFactory configurationFactory) {
        this.configurationFactory = configurationFactory;
    }

    @Override
    public void initialize(RequestContext context) {
        super.initialize(context);

        // Check if the underlying configuration is for HDFS
        hcfsType = HcfsType.getHcfsType(configuration, context);
        jobConf = new JobConf(configuration, this.getClass());
    }

    @Override
    public List<Fragment> getFragments() throws Exception {

        /*
            HCFS-based fragmenters need to use retry logic in Kerberos secured clusters
            to overcome potential error with GSS initiate failures related to replay attacks.
         */
        return fetchFragmentsWithRetry();
    }

    protected List<Fragment> fetchFragmentsWithRetry() throws Exception {
        // max attempts to fetch fragments is 1 unless we have Kerberos-secured cluster that needs retries to
        // overcome potential SASL AUTH failures
        boolean securityEnabled = Utilities.isSecurityEnabled(configuration);
        int maxAttempts = securityEnabled ?
                configuration.getInt("pxf.sasl.connection.retries", 5) + 1 : 1;

        LOG.debug("Before fetching fragments, security = {}, max attempts = {}", securityEnabled, maxAttempts);

        // retry upto max allowed number
        List<Fragment> result = null;
        for (int attempt = 0; attempt < maxAttempts; attempt++) {
            try {
                LOG.debug("Fetching fragments attempt #{} of {}", attempt, maxAttempts);
                result = fetchFragments();
                break;
            } catch (IOException e) {
                if (StringUtils.contains(e.getMessage(), "GSS initiate failed")) {
                    LOG.warn(String.format("Attempt #%d failed to fetch fragments: ", attempt), e.getMessage());
                    if (attempt < maxAttempts - 1) {
                        int backoffMs = new Random().nextInt(MAX_BACKOFF) + 1;
                        LOG.debug("Backing off for {} ms before the next attempt", backoffMs);
                        Thread.sleep(backoffMs);
                    } else {
                        // reached the max allowed number of retires, re-throw the exception to the caller
                        throw e;
                    }
                } else {
                    // non-GSS initiate failed IOException needs to be rethrown.
                    throw e;
                }
            }
        }
        return result;
    }

    /**
     * Gets the fragments for a data source URI that can appear as a file name,
     * a directory name or a wildcard. Returns the data fragments in JSON
     * format.
     */
    protected List<Fragment> fetchFragments() throws Exception {
        Path path = new Path(hcfsType.getDataUri(jobConf, context));
        List<InputSplit> splits;
        try {
            splits = getSplits(path);
        } catch (InvalidInputException e) {
            if (StringUtils.equalsIgnoreCase("true", context.getOption(IGNORE_MISSING_PATH_OPTION))) {
                LOG.debug("Ignoring InvalidInputException", e);
                return fragments;
            }
            throw e;
        }

        LOG.debug("Total number of fragments = {}", splits.size());
        for (InputSplit split : splits) {
            FileSplit fsp = (FileSplit) split;
            String filepath = fsp.getPath().toString();
            String[] hosts = fsp.getLocations();

            /*
             * metadata information includes: file split's start, length and
             * hosts (locations).
             */
            byte[] fragmentMetadata = HdfsUtilities.prepareFragmentMetadata(fsp);
            Fragment fragment = new Fragment(filepath, hosts, fragmentMetadata);
            fragments.add(fragment);
        }

        return fragments;
    }

    @Override
    public FragmentStats getFragmentStats() throws Exception {
        String absoluteDataPath = hcfsType.getDataUri(jobConf, context);
        List<InputSplit> splits = getSplits(new Path(absoluteDataPath));

        if (splits.isEmpty()) {
            return new FragmentStats(0, 0, 0);
        }
        long totalSize = 0;
        for (InputSplit split : splits) {
            totalSize += split.getLength();
        }
        InputSplit firstSplit = splits.get(0);
        return new FragmentStats(splits.size(), firstSplit.getLength(), totalSize);
    }

    protected List<InputSplit> getSplits(Path path) throws IOException {
        PxfInputFormat pxfInputFormat = new PxfInputFormat();
        PxfInputFormat.setInputPaths(jobConf, path);
        InputSplit[] splits = pxfInputFormat.getSplits(jobConf, 1);
        List<InputSplit> result = new ArrayList<>();

        /*
         * HD-2547: If the file is empty, an empty split is returned: no
         * locations and no length.
         */
        if (splits != null) {
            for (InputSplit split : splits) {
                if (split.getLength() > 0) {
                    result.add(split);
                }
            }
        }

        return result;
    }

}
