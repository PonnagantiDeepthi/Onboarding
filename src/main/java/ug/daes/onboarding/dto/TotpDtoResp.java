package ug.daes.onboarding.dto;

import java.io.Serializable;

public class TotpDtoResp implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String authData;
	private String priauthscheme;
	public String getAuthData() {
		return authData;
	}
	public void setAuthData(String authData) {
		this.authData = authData;
	}
	public String getPriauthscheme() {
		return priauthscheme;
	}
	public void setPriauthscheme(String priauthscheme) {
		this.priauthscheme = priauthscheme;
	}
	@Override
	public String toString() {
		return "TotpDtoResp [authData=" + authData + ", priauthscheme=" + priauthscheme + "]";
	}
}
