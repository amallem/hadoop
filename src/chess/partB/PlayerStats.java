package chess.partB;

/**
 * Created by anirudh on 12/10/15.
 */
public class PlayerStats {

    private String playerName;

    private String playerColour;

    private double gamesWon;

    private double gamesLost;

    private double gamesDraw;

    private double totalGames;

    public PlayerStats() {
        this.playerName = "";
        this.playerColour = "";
        this.gamesWon = 0;
        this.gamesLost = 0;
        this.gamesDraw = 0;
    }

    public void reset() {
        this.playerName = "";
        this.playerColour = "";
        this.gamesWon = 0;
        this.gamesLost = 0;
        this.gamesDraw = 0;
    }

    public void setPlayerName(String name) {
        this.playerName = name;
    }

    public void setPlayerColour(String colour) {
        this.playerColour = colour;
    }

    public void setGamesWon(double numGames) {
        this.gamesWon = numGames;
    }

    public void setGamesLost(double numGames) {
        this.gamesLost = numGames;
    }

    public void setGamesDraw(double numGames) {
        this.gamesDraw = numGames;
    }

    public void setTotalGames(double numgames) {
        this.totalGames = numgames;
    }

    public String getPlayerName() {
        return this.playerName;
    }

    public String getPlayerColour() {
        return this.playerColour;
    }

    public double getGamesWon() {
        return this.gamesWon;
    }

    public double getGamesLost() {
        return this.gamesLost;
    }

    public double getGamesDraw() {
        return this.gamesDraw;
    }

    public double getTotalGames() {
        return this.totalGames;
    }

    public void setGameStats(String result, String numGames) {
        double games = Double.parseDouble(numGames);
        switch (result) {
            case "Won":
                setGamesWon(games);
                break;
            case "Draw":
                setGamesDraw(games);
                break;
            case "Loss":
                setGamesLost(games);
                break;
            default:
                break;
        }
    }

    public void setPercentages() {
        setGamesWon(getGamesWon() / getTotalGames());
        setGamesLost(getGamesLost() / getTotalGames());
        setGamesDraw(getGamesDraw() / getTotalGames());
    }
}
