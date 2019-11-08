package kafka101.events;

public class Event {
    int userID;
    EventType eventType;
    int amount;

    public int getUserID(){return userID;}
    public EventType getEventType(){return eventType;}
    public int getAmount(){return amount;}

    @Override
    public String toString() {
        return "User ID: " + this.userID + " type of transaction: " + this.eventType + " amount: " + this.amount;
    }
}
