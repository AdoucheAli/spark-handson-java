package fr.devoxx.entity;

public class Performer {

    private String performer;
    private String style;

    public Performer(String performer, String style) {
        this.performer = performer;
        this.style = style;
    }

    public String getPerformer() {
        return performer;
    }

    public void setPerformer(String performer) {
        this.performer = performer;
    }

    public String getStyle() {
        return style;
    }

    public void setStyle(String style) {
        this.style = style;
    }
}
