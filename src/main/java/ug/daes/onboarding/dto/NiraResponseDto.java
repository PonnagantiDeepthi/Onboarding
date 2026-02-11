package ug.daes.onboarding.dto;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class NiraResponseDto implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String photo;
	private String cardStatus;
	private String cardExpiryDate;
	private String cardNumber;
	private String nationality;
	private String nationalId;
	private String surname;
	private String givenNames;
	private String dateOfBirth;
	private boolean dateOfBirthEstimated;
	private String gender;
	
	
	
	public String getPhoto() {
		return photo;
	}



	public void setPhoto(String photo) {
		this.photo = photo;
	}



	public String getCardStatus() {
		return cardStatus;
	}



	public void setCardStatus(String cardStatus) {
		this.cardStatus = cardStatus;
	}



	public String getCardExpiryDate() {
		return cardExpiryDate;
	}



	public void setCardExpiryDate(String cardExpiryDate) {
		this.cardExpiryDate = cardExpiryDate;
	}



	public String getCardNumber() {
		return cardNumber;
	}



	public void setCardNumber(String cardNumber) {
		this.cardNumber = cardNumber;
	}



	public String getNationality() {
		return nationality;
	}



	public void setNationality(String nationality) {
		this.nationality = nationality;
	}



	public String getNationalId() {
		return nationalId;
	}



	public void setNationalId(String nationalId) {
		this.nationalId = nationalId;
	}



	public String getSurname() {
		return surname;
	}



	public void setSurname(String surname) {
		this.surname = surname;
	}



	public String getGivenNames() {
		return givenNames;
	}



	public void setGivenNames(String givenNames) {
		this.givenNames = givenNames;
	}



	public String getDateOfBirth() {
		return dateOfBirth;
	}



	public void setDateOfBirth(String dateOfBirth) {
		this.dateOfBirth = dateOfBirth;
	}



	public boolean isDateOfBirthEstimated() {
		return dateOfBirthEstimated;
	}



	public void setDateOfBirthEstimated(boolean dateOfBirthEstimated) {
		this.dateOfBirthEstimated = dateOfBirthEstimated;
	}



	public String getGender() {
		return gender;
	}



	public void setGender(String gender) {
		this.gender = gender;
	}



	@Override
    public String toString() {
        return "NiraResponseDto{" +
                "photo='" + photo + '\'' +
                ", cardStatus='" + cardStatus + '\'' +
                ", cardExpiryDate='" + cardExpiryDate + '\'' +
                ", cardNumber='" + cardNumber + '\'' +
                ", nationality='" + nationality + '\'' +
                ", nationalId='" + nationalId + '\'' +
                ", surname='" + surname + '\'' +
                ", givenNames='" + givenNames + '\'' +
                ", dateOfBirth='" + dateOfBirth + '\'' +
                ", dateOfBirthEstimated=" + dateOfBirthEstimated +
                ", gender='" + gender + '\'' +
                '}';
    }
}
