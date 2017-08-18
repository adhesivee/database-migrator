package nl.myndocs.database.migrator.database.query;

/**
 * Created by albert on 18-8-2017.
 */
@FunctionalInterface
public interface PhraseTranslator {
    String[] translatePhrases(Query query, Phrase... phrases);
}
