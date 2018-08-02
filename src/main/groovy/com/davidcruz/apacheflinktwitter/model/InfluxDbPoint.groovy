package com.davidcruz.apacheflinktwitter.model

import groovy.transform.Canonical

@Canonical
class InfluxDbPoint {
    String measurement
    long timestamp
    Map<String, String> tags
    Map<String, Object> fields
}
