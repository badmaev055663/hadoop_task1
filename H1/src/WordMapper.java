import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;
import org.apache.lucene.analysis.*;

import java.io.*;
import java.util.*;

public class WordMapper extends Mapper<Object, Text, Text, IntWritable>
{
    public void map(Object key, Text value, Context context)
    throws IOException, InterruptedException {
        CharArraySet englishStopWords = EnglishAnalyzer.getDefaultStopSet();
        CharArraySet frenchStopWords = FrenchAnalyzer.getDefaultStopSet();
        CharArraySet russianStopWords = RussianAnalyzer.getDefaultStopSet();
        CharArraySet germanStopWords = GermanAnalyzer.getDefaultStopSet();

        CharArraySet stopWords = new CharArraySet(englishStopWords, true);
        stopWords.addAll(englishStopWords);
        stopWords.addAll(frenchStopWords);
        stopWords.addAll(russianStopWords);
        stopWords.addAll(germanStopWords);

        StandardAnalyzer analyzer = new StandardAnalyzer(stopWords);

        TokenStream tokenStream = new StopFilter(analyzer.tokenStream("fieldName", value.toString()),
                analyzer.getStopwordSet());
        CharTermAttribute attr = tokenStream.addAttribute(CharTermAttribute.class);
        tokenStream.reset();
        while (tokenStream.incrementToken()) {
            context.write(new Text(attr.toString()), new IntWritable(1));
        }
    }
}