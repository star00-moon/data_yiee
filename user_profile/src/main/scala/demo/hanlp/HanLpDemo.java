package demo.hanlp;

import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.dictionary.py.Pinyin;
import com.hankcs.hanlp.seg.common.Term;

import java.util.List;

/**
 * @author: 余辉
 * @blog:   https://blog.csdn.net/silentwolfyh
 * @create: 2019/10/22
 * @description:  HanLp自然语言处理工具包应用示例
 **/

public class HanLpDemo {
    public static void main(String[] args) {

        // 1、文字转换
        String doc = "我有一头小毛驴我从来也不骑，有一天我心血来潮骑着去赶集";

        // 1-1、转化为拼音（首字母） convertToPinyinFirstCharString(文本，分隔符，false)
        String s = HanLP.convertToPinyinFirstCharString(doc, " ", false);
        System.out.println(s);

        // 1-2、转化为拼音 convertToPinyinList（文本）    [wo3, you3, yi1] 后面的数字是音调
        List<Pinyin> pinyins = HanLP.convertToPinyinList(doc);
        System.out.println(pinyins);

        // 1-3、简转繁  convertToTraditionalChinese（文本）
        String s2 = HanLP.convertToTraditionalChinese(doc);
        System.out.println(s2);

        // 2、文字提取
        String doc2 = "娱乐圈中总少不了各路明星之间的“暗战大戏”，而近段时间闹得最沸沸扬扬的非张韶涵和范玮琪莫属。自从张韶涵事业回春之后，当初在低谷时的种种旧事就被翻了出来，当时与张韶涵之间是非难断的范玮琪自然就首当其冲成了粉丝们发泄的目标。\n" +
                "张韶涵发问号\n" +
                "    近日，针对之前的一系列负面消息，范玮琪在社交账号上开始了为自己正名的操作，回复了很多对自己有负面评价的网友，最后更是情绪大爆发问自己到底做错了什么。而这其中争议最多的，还是其与张韶涵之间的旧事。不少网友表示，张韶涵和范玮琪这段剪不断理还乱的昔日姐妹情一直上演了许多年，直到现在还没演完，看来两个人在未来也很可能要继续“捆绑”了。\n" +
                "\n";

        // 2-1、提取关键词  extractKeyword(doc，个数)
        List<String> keywords = HanLP.extractKeyword(doc2, 8);
        System.out.println(keywords);

        // 2-2、自动摘要 extractSummary(doc , 个数)
        List<String> summary = HanLP.extractSummary(doc2, 4);
        System.out.println(summary);

        //2-3、分词    segment（文本）
        List<Term> terms = HanLP.segment(doc);
        for (Term term : terms) {
            System.out.println(term.word);
        }
    }
}
