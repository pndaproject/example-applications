/**
  * Name:       NetworkData
  * Purpose:    This is to read the raw data ( json string ) and map it to appropriate fields to use further.
  * Author:     PNDA team
  *
  * Created:    30/05/2018
  */

package com.cisco.pnda.examples.util;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

public class NetworkData {

	@JsonProperty("hostname")
	private String hostname;

	@JsonProperty("nw_interface")
	private String nwInterface;

	@JsonProperty("RX_Bytes")
	private Long rxBytes;

	@JsonProperty("TX_Bytes")
	private Long txBytes;

	@JsonProperty("timestamp")
	private Long timestamp;

	public NetworkData() {
	}

	public NetworkData(String hostname, String nwInterface, Long rxBytes, Long txBytes, Long timestamp) {
		super();
		this.hostname = hostname;
		this.nwInterface = nwInterface;
		this.rxBytes = rxBytes;
		this.txBytes = txBytes;
		this.timestamp = timestamp;
	}

	public String getHostname() {
		return hostname;
	}

	public void setHostname(String hostname) {
		this.hostname = hostname;
	}

	public String getNwInterface() {
		return nwInterface;
	}

	public void setNwInterface(String nwInterface) {
		this.nwInterface = nwInterface;
	}

	public Long getRxBytes() {
		return rxBytes;
	}

	public void setRxBytes(Long rxBytes) {
		this.rxBytes = rxBytes;
	}

	public Long getTxBytes() {
		return txBytes;
	}

	public void setTxBytes(Long txBytes) {
		this.txBytes = txBytes;
	}

	public Long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}
}
