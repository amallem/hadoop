
/**
 * Created by anirudh on 12/8/15.
 */
public class PGNGame {



    private String whitePlayer;
    private String blackPlayer;
    private int plyCount;
    private int result;

    public PGNGame(String white, String black, int plyCOunt, int result){
        whitePlayer = white;
        blackPlayer = black;
        this.plyCount = plyCOunt;
        this.result = result;
    }

    public String getWhitePlayer(){
        return this.whitePlayer;
    }

    public String getBlacklayer(){
        return this.blackPlayer;
    }

    public int getPlyCount(){
        return this.plyCount;
    }

    public int getResult(){
        return this.result;
    }
}
