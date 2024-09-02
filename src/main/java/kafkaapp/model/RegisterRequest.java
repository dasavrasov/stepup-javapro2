package kafkaapp.model;

public class RegisterRequest {
    private String name;
    private String ConferenceID;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getConferenceID() {
        return ConferenceID;
    }

    public void setConferenceID(String conferenceID) {
        ConferenceID = conferenceID;
    }
}