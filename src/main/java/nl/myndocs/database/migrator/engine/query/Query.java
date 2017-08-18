package nl.myndocs.database.migrator.engine.query;

import nl.myndocs.database.migrator.definition.Column;
import nl.myndocs.database.migrator.definition.Constraint;
import nl.myndocs.database.migrator.definition.ForeignKey;
import nl.myndocs.database.migrator.definition.Table;

import java.util.ArrayList;
import java.util.List;

public class Query {


    private final PhraseTranslator phraseTranslator;
    private Table table;
    private Column column;
    private Constraint constraint;
    private ForeignKey foreignKey;
    private List<Phrase> phrases = new ArrayList<>();
    private String columnName;
    private String constraintName;

    public Query(PhraseTranslator phraseTranslator) {
        this.phraseTranslator = phraseTranslator;
    }

    // @TODO Probably should be removed
    public Query newCleanInstance() {
        return new Query(phraseTranslator);
    }

    public Query query(Phrase... phrases) {
        this.phrases.clear();

        for (Phrase phrase : phrases) {
            this.phrases.add(phrase);
        }

        return this;
    }

    public Query setTable(Table table) {
        this.table = table;

        return this;
    }

    public Query setColumn(Column column) {
        this.column = column;

        return this;
    }

    public Query setForeignKey(ForeignKey foreignKey) {
        this.foreignKey = foreignKey;

        return this;
    }

    public Query setConstraint(Constraint constraint) {
        this.constraint = constraint;

        return this;
    }

    public String getColumnName() {
        return columnName;
    }

    public Query setColumnName(String columnName) {
        this.columnName = columnName;

        return this;
    }

    public Table getTable() {
        return table;
    }

    public Column getColumn() {
        return column;
    }

    public Constraint getConstraint() {
        return constraint;
    }

    public ForeignKey getForeignKey() {
        return foreignKey;
    }

    public List<Phrase> getPhrases() {
        return phrases;
    }

    public boolean endsWith(Phrase... matchPhrases) {
        boolean match = true;

        int phrasesOffset = this.phrases.size() - 1;
        for (int i = (matchPhrases.length - 1); i >= 0; i--, phrasesOffset--) {
            if (!this.phrases.get(phrasesOffset).equals(matchPhrases[i])) {
                match = false;
            }
        }

        return match;
    }

    public boolean equals(Phrase... matchPhrases) {
        boolean match = true;

        if (matchPhrases.length != phrases.size()) {
            match = false;
        }

        for (int i = 0; i < phrases.size(); i++) {
            if (!this.phrases.get(i).equals(matchPhrases[i])) {
                match = false;
            }
        }

        return match;
    }

    public Query setConstraintName(String constraintName) {
        this.constraintName = constraintName;

        return this;
    }

    public String getConstraintName() {
        return constraintName;
    }

    public String[] getQueries() {
        StringBuilder stringBuilder = new StringBuilder();

        List<Phrase> phrases = getPhrases();
        return phraseTranslator.translatePhrases(
                this,
                phrases.toArray(new Phrase[phrases.size()])
        );
    }
}
