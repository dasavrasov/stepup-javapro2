package kafkaapp.model;

import java.util.List;

public class NewMembersResponse {
    private List<String> names;

    public List<String> getNames() {
        return names;
    }

    public void setNames(List<String> names) {
        this.names = names;
    }
}