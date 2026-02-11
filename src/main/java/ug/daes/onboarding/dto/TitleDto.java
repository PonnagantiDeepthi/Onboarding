package ug.daes.onboarding.dto;

import java.io.Serializable;

public class TitleDto implements Serializable {
    private static final long serialVersionUID = 1L;


    private String suid;

    private String title;

    public String getSuid() {
        return suid;
    }

    public void setSuid(String suid) {
        this.suid = suid;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    @Override
    public String toString() {
        return "TitleDto{" +
                "suid='" + suid + '\'' +
                ", title='" + title + '\'' +
                '}';
    }
}
