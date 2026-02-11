package ug.daes.onboarding.model;

import jakarta.persistence.*;
import java.io.Serializable;

@Entity
@Table(name = "simulated_boarder_control")  // Make sure this matches your actual table name
public class SimulatedBoarderControl implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")  // Assume there is a primary key column named "id"
    private Long id;

    @Column(name = "id_doc_number")
    private String idDocNumber;

    @Column(name = "selfie_image", columnDefinition = "TEXT")  // Assuming selfieImage is stored as large text (e.g., base64 or URL)
    private String selfieImage;

    public SimulatedBoarderControl() {
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getIdDocNumber() {
        return idDocNumber;
    }

    public void setIdDocNumber(String idDocNumber) {
        this.idDocNumber = idDocNumber;
    }

    public String getSelfieImage() {
        return selfieImage;
    }

    public void setSelfieImage(String selfieImage) {
        this.selfieImage = selfieImage;
    }

    @Override
    public String toString() {
        return "SimulatedBoarderControl{" +
                "id=" + id +
                ", idDocNumber='" + idDocNumber + '\'' +
                ", selfieImage='" + selfieImage + '\'' +
                '}';
    }
}

