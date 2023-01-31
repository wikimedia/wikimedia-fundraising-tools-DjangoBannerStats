######################################################################################
# T198639: pgehres: Remove obviously incorrect rows from lookup tables               #
# T277484: Fix duplicate entries in pgehres tables so we can convert them to utf8mb4 #
# T301905: modernize DjangoBannerStats to python3                                    #
######################################################################################

SET character_set_client="utf8mb4"

###########################
# PURGE DEPRECATED TABLES #
###########################

DROP TABLE IF EXISTS auth_group;
DROP TABLE IF EXISTS auth_group_permissions;
DROP TABLE IF EXISTS auth_permission;
DROP TABLE IF EXISTS auth_user;
DROP TABLE IF EXISTS auth_user_groups;
DROP TABLE IF EXISTS auth_user_user_permissions;
DROP TABLE IF EXISTS django_content_type;
DROP TABLE IF EXISTS django_session;
DROP TABLE IF EXISTS django_site;
DROP TABLE IF EXISTS kp_queue2civicrm_log;
DROP TABLE IF EXISTS optout;

#########################################################
# MIGRATE COUNTRY TABLE TO UTF8MB4, PURGING JUNK VALUES #
#########################################################

CREATE TABLE country_new (
  id smallint(3) unsigned NOT NULL AUTO_INCREMENT,
  country varchar(128) NOT NULL DEFAULT '',
  iso_code varchar(8) NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY iso_code (iso_code)
) ENGINE=InnoDB AUTO_INCREMENT=2608 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

# import countries with valid ISO country codes, referencing list from civicrm
INSERT INTO country_new SELECT c.id,name,UPPER(c.iso_code) AS iso_new FROM civicrm.civicrm_country cc, country c WHERE cc.iso_code=c.iso_code; 

# swap tables
RENAME TABLE country TO country_old;
RENAME TABLE country_new TO country;

##########################################################
# MIGRATE LANGUAGE TABLE TO UTF8MB4, PURGING JUNK VALUES #
##########################################################

CREATE TABLE language_new (
  id smallint(3) unsigned NOT NULL AUTO_INCREMENT,
  language varchar(128) NOT NULL DEFAULT '',
  iso_code varchar(24) NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY iso_code (iso_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

# import only rows with proper ISO country code, using the list from mediawiki includes/languages/data/Names.php
INSERT INTO language_new SELECT * FROM language WHERE iso_code IN ('aa','ab','abs','ace','ady','ady-cyrl','aeb','aeb-arab','aeb-latn','af','ak','aln','als','alt','am','ami','an','ang','anp','ar','arc','arn','arq','ary','arz','as','ase','ast','atj','av','avk','awa','ay','az','azb','ba','ban','ban-bali','bar','bat-smg','bbc','bbc-latn','bcc','bci','bcl','be','be-tarask','be-x-old','bg','bgn','bh','bho','bi','bjn','blk','bm','bn','bo','bpy','bqi','br','brh','bs','btm','bto','bug','bxr','ca','cbk-zam','cdo','ce','ceb','ch','cho','chr','chy','ckb','co','cps','cr','crh','crh-cyrl','crh-latn','cs','csb','cu','cv','cy','da','dag','de','de-at','de-ch','de-formal','din','diq','dsb','dtp','dty','dv','dz','ee','egl','el','eml','en','en-ca','en-gb','eo','es','es-419','es-formal','et','eu','ext','fa','fat','ff','fi','fit','fiu-vro','fj','fo','fon','fr','frc','frp','frr','fur','fy','ga','gaa','gag','gan','gan-hans','gan-hant','gcr','gd','gl','gld','glk','gn','gom','gom-deva','gom-latn','gor','got','gpe','grc','gsw','gu','guc','gur','guw','gv','ha','hak','haw','he','hi','hif','hif-latn','hil','ho','hr','hrx','hsb','hsn','ht','hu','hu-formal','hy','hyw','hz','ia','id','ie','ig','igl','ii','ik','ike-cans','ike-latn','ilo','inh','io','is','it','iu','ja','jam','jbo','jut','jv','ka','kaa','kab','kbd','kbd-cyrl','kbp','kcg','kea','kg','khw','ki','kiu','kj','kjh','kjp','kk','kk-arab','kk-cn','kk-cyrl','kk-kz','kk-latn','kk-tr','kl','km','kn','ko','ko-kp','koi','kr','krc','kri','krj','krl','ks','ks-arab','ks-deva','ksh','ksw','ku','ku-arab','ku-latn','kum','kv','kw','ky','la','lad','lb','lbe','lez','lfn','lg','li','lij','liv','lki','lld','lmo','ln','lo','loz','lrc','lt','ltg','lus','luz','lv','lzh','lzz','mad','mag','mai','map-bms','mdf','mg','mh','mhr','mi','min','mk','ml','mn','mni','mnw','mo','mos','mr','mrh','mrj','ms','ms-arab','mt','mus','mwl','my','myv','mzn','na','nah','nan','nap','nb','nds','nds-nl','ne','new','ng','nia','niu','nl','nl-informal','nmz','nn','no','nod','nov','nqo','nrm','nso','nv','ny','nyn','nys','oc','ojb','olo','om','or','os','pa','pag','pam','pap','pcd','pcm','pdc','pdt','pfl','pi','pih','pl','pms','pnb','pnt','prg','ps','pt','pt-br','pwn','qu','qug','rgn','rif','rki','rm','rmc','rmy','rn','ro','roa-rup','roa-tara','rsk','ru','rue','rup','ruq','ruq-cyrl','ruq-latn','rw','ryu','sa','sah','sat','sc','scn','sco','sd','sdc','sdh','se','se-fi','se-no','se-se','sei','ses','sg','sgs','sh','sh-cyrl','sh-latn','shi','shi-latn','shi-tfng','shn','shy','shy-latn','si','simple','sjd','sje','sk','skr','skr-arab','sl','sli','sm','sma','smn','sms','sn','so','sq','sr','sr-ec','sr-el','srn','sro','ss','st','stq','sty','su','sv','sw','syl','szl','szy','ta','tay','tcy','tdd','te','tet','tg','tg-cyrl','tg-latn','th','ti','tk','tl','tly','tly-cyrl','tn','to','tok','tpi','tr','tru','trv','ts','tt','tt-cyrl','tt-latn','tum','tw','ty','tyv','tzm','udm','ug','ug-arab','ug-latn','uk','ur','uz','uz-cyrl','uz-latn','ve','vec','vep','vi','vls','vmf','vmw','vo','vot','vro','wa','war','wls','wo','wuu','xal','xh','xmf','xsy','yi','yo','yrl','yue','za','zea','zgh','zh','zh-classical','zh-cn','zh-hans','zh-hant','zh-hk','zh-min-nan','zh-mo','zh-my','zh-sg','zh-tw','zh-yue','zu','other');

UPDATE language_new SET language="Qafár af" WHERE iso_code="aa";
UPDATE language_new SET language="аԥсшәа" WHERE iso_code="ab";
UPDATE language_new SET language="bahasa ambon" WHERE iso_code="abs";
UPDATE language_new SET language="Acèh" WHERE iso_code="ace";
UPDATE language_new SET language="адыгабзэ" WHERE iso_code="ady";
UPDATE language_new SET language="адыгабзэ" WHERE iso_code="ady-cyrl";
UPDATE language_new SET language="تونسي/Tûnsî" WHERE iso_code="aeb";
UPDATE language_new SET language="تونسي" WHERE iso_code="aeb-arab";
UPDATE language_new SET language="Tûnsî" WHERE iso_code="aeb-latn";
UPDATE language_new SET language="Afrikaans" WHERE iso_code="af";
UPDATE language_new SET language="Akan" WHERE iso_code="ak";
UPDATE language_new SET language="Gegë" WHERE iso_code="aln";
UPDATE language_new SET language="Alemannisch" WHERE iso_code="als";
UPDATE language_new SET language="алтай тил" WHERE iso_code="alt";
UPDATE language_new SET language="አማርኛ" WHERE iso_code="am";
UPDATE language_new SET language="Pangcah" WHERE iso_code="ami";
UPDATE language_new SET language="aragonés" WHERE iso_code="an";
UPDATE language_new SET language="Ænglisc" WHERE iso_code="ang";
UPDATE language_new SET language="अंगिका" WHERE iso_code="anp";
UPDATE language_new SET language="العربية" WHERE iso_code="ar";
UPDATE language_new SET language="ܐܪܡܝܐ" WHERE iso_code="arc";
UPDATE language_new SET language="mapudungun" WHERE iso_code="arn";
UPDATE language_new SET language="جازايرية" WHERE iso_code="arq";
UPDATE language_new SET language="الدارجة" WHERE iso_code="ary";
UPDATE language_new SET language="مصرى" WHERE iso_code="arz";
UPDATE language_new SET language="অসমীয়া" WHERE iso_code="as";
UPDATE language_new SET language="American sign language" WHERE iso_code="ase";
UPDATE language_new SET language="asturianu" WHERE iso_code="ast";
UPDATE language_new SET language="Atikamekw" WHERE iso_code="atj";
UPDATE language_new SET language="авар" WHERE iso_code="av";
UPDATE language_new SET language="Kotava" WHERE iso_code="avk";
UPDATE language_new SET language="अवधी" WHERE iso_code="awa";
UPDATE language_new SET language="Aymar aru" WHERE iso_code="ay";
UPDATE language_new SET language="azərbaycanca" WHERE iso_code="az";
UPDATE language_new SET language="تۆرکجه" WHERE iso_code="azb";
UPDATE language_new SET language="башҡортса" WHERE iso_code="ba";
UPDATE language_new SET language="Basa Bali" WHERE iso_code="ban";
UPDATE language_new SET language="ᬩᬲᬩᬮᬶ" WHERE iso_code="ban-bali";
UPDATE language_new SET language="Boarisch" WHERE iso_code="bar";
UPDATE language_new SET language="žemaitėška" WHERE iso_code="bat-smg";
UPDATE language_new SET language="Batak Toba" WHERE iso_code="bbc";
UPDATE language_new SET language="Batak Toba" WHERE iso_code="bbc-latn";
UPDATE language_new SET language="جهلسری بلوچی" WHERE iso_code="bcc";
UPDATE language_new SET language="wawle" WHERE iso_code="bci";
UPDATE language_new SET language="Bikol Central" WHERE iso_code="bcl";
UPDATE language_new SET language="беларуская" WHERE iso_code="be";
UPDATE language_new SET language="беларуская тарашкевіца" WHERE iso_code="be-tarask";
UPDATE language_new SET language="беларуская тарашкевіца" WHERE iso_code="be-x-old";
UPDATE language_new SET language="български" WHERE iso_code="bg";
UPDATE language_new SET language="روچ کپتین بلوچی" WHERE iso_code="bgn";
UPDATE language_new SET language="भोजपुरी" WHERE iso_code="bh";
UPDATE language_new SET language="भोजपुरी" WHERE iso_code="bho";
UPDATE language_new SET language="Bislama" WHERE iso_code="bi";
UPDATE language_new SET language="Banjar" WHERE iso_code="bjn";
UPDATE language_new SET language="ပအိုဝ်ႏဘာႏသာႏ" WHERE iso_code="blk";
UPDATE language_new SET language="bamanankan" WHERE iso_code="bm";
UPDATE language_new SET language="বাংলা" WHERE iso_code="bn";
UPDATE language_new SET language="བོད་ཡིག" WHERE iso_code="bo";
UPDATE language_new SET language="বিষ্ণুপ্রিয়া মণিপুরী" WHERE iso_code="bpy";
UPDATE language_new SET language="بختیاری" WHERE iso_code="bqi";
UPDATE language_new SET language="brezhoneg" WHERE iso_code="br";
UPDATE language_new SET language="Bráhuí" WHERE iso_code="brh";
UPDATE language_new SET language="bosanski" WHERE iso_code="bs";
UPDATE language_new SET language="Batak Mandailing" WHERE iso_code="btm";
UPDATE language_new SET language="Iriga Bicolano" WHERE iso_code="bto";
UPDATE language_new SET language="ᨅᨔ ᨕᨘᨁᨗ" WHERE iso_code="bug";
UPDATE language_new SET language="буряад" WHERE iso_code="bxr";
UPDATE language_new SET language="català" WHERE iso_code="ca";
UPDATE language_new SET language="Chavacano de Zamboanga" WHERE iso_code="cbk-zam";
UPDATE language_new SET language="閩東語 / Mìng-dĕ̤ng-ngṳ̄" WHERE iso_code="cdo";
UPDATE language_new SET language="нохчийн" WHERE iso_code="ce";
UPDATE language_new SET language="Cebuano" WHERE iso_code="ceb";
UPDATE language_new SET language="Chamoru" WHERE iso_code="ch";
UPDATE language_new SET language="Chahta Anumpa" WHERE iso_code="cho";
UPDATE language_new SET language="ᏣᎳᎩ" WHERE iso_code="chr";
UPDATE language_new SET language="Tsetsêhestâhese" WHERE iso_code="chy";
UPDATE language_new SET language="کوردی" WHERE iso_code="ckb";
UPDATE language_new SET language="corsu" WHERE iso_code="co";
UPDATE language_new SET language="Capiceño" WHERE iso_code="cps";
UPDATE language_new SET language="Nēhiyawēwin / ᓀᐦᐃᔭᐍᐏᐣ" WHERE iso_code="cr";
UPDATE language_new SET language="qırımtatarca" WHERE iso_code="crh";
UPDATE language_new SET language="къырымтатарджа Кирилл" WHERE iso_code="crh-cyrl";
UPDATE language_new SET language="qırımtatarca Latin" WHERE iso_code="crh-latn";
UPDATE language_new SET language="čeština" WHERE iso_code="cs";
UPDATE language_new SET language="kaszëbsczi" WHERE iso_code="csb";
UPDATE language_new SET language="словѣньскъ / ⰔⰎⰑⰂⰡⰐⰠⰔⰍⰟ" WHERE iso_code="cu";
UPDATE language_new SET language="чӑвашла" WHERE iso_code="cv";
UPDATE language_new SET language="Cymraeg" WHERE iso_code="cy";
UPDATE language_new SET language="dansk" WHERE iso_code="da";
UPDATE language_new SET language="dagbanli" WHERE iso_code="dag";
UPDATE language_new SET language="Deutsch" WHERE iso_code="de";
UPDATE language_new SET language="Österreichisches Deutsch" WHERE iso_code="de-at";
UPDATE language_new SET language="Schweizer Hochdeutsch" WHERE iso_code="de-ch";
UPDATE language_new SET language="Deutsch Sie-Form" WHERE iso_code="de-formal";
UPDATE language_new SET language="Thuɔŋjäŋ" WHERE iso_code="din";
UPDATE language_new SET language="Zazaki" WHERE iso_code="diq";
UPDATE language_new SET language="dolnoserbski" WHERE iso_code="dsb";
UPDATE language_new SET language="Dusun Bundu-liwan" WHERE iso_code="dtp";
UPDATE language_new SET language="डोटेली" WHERE iso_code="dty";
UPDATE language_new SET language="ދިވެހިބަސް" WHERE iso_code="dv";
UPDATE language_new SET language="ཇོང་ཁ" WHERE iso_code="dz";
UPDATE language_new SET language="eʋegbe" WHERE iso_code="ee";
UPDATE language_new SET language="Emiliàn" WHERE iso_code="egl";
UPDATE language_new SET language="Ελληνικά" WHERE iso_code="el";
UPDATE language_new SET language="emiliàn e rumagnòl" WHERE iso_code="eml";
UPDATE language_new SET language="English" WHERE iso_code="en";
UPDATE language_new SET language="Canadian English" WHERE iso_code="en-ca";
UPDATE language_new SET language="British English" WHERE iso_code="en-gb";
UPDATE language_new SET language="Esperanto" WHERE iso_code="eo";
UPDATE language_new SET language="español" WHERE iso_code="es";
UPDATE language_new SET language="español de América Latina" WHERE iso_code="es-419";
UPDATE language_new SET language="español formal" WHERE iso_code="es-formal";
UPDATE language_new SET language="eesti" WHERE iso_code="et";
UPDATE language_new SET language="euskara" WHERE iso_code="eu";
UPDATE language_new SET language="estremeñu" WHERE iso_code="ext";
UPDATE language_new SET language="فارسی" WHERE iso_code="fa";
UPDATE language_new SET language="mfantse" WHERE iso_code="fat";
UPDATE language_new SET language="Fulfulde" WHERE iso_code="ff";
UPDATE language_new SET language="suomi" WHERE iso_code="fi";
UPDATE language_new SET language="meänkieli" WHERE iso_code="fit";
UPDATE language_new SET language="võro" WHERE iso_code="fiu-vro";
UPDATE language_new SET language="Na Vosa Vakaviti" WHERE iso_code="fj";
UPDATE language_new SET language="føroyskt" WHERE iso_code="fo";
UPDATE language_new SET language="fɔ̀ngbè" WHERE iso_code="fon";
UPDATE language_new SET language="français" WHERE iso_code="fr";
UPDATE language_new SET language="français cadien" WHERE iso_code="frc";
UPDATE language_new SET language="arpetan" WHERE iso_code="frp";
UPDATE language_new SET language="Nordfriisk" WHERE iso_code="frr";
UPDATE language_new SET language="furlan" WHERE iso_code="fur";
UPDATE language_new SET language="Frysk" WHERE iso_code="fy";
UPDATE language_new SET language="Gaeilge" WHERE iso_code="ga";
UPDATE language_new SET language="Ga" WHERE iso_code="gaa";
UPDATE language_new SET language="Gagauz" WHERE iso_code="gag";
UPDATE language_new SET language="贛語" WHERE iso_code="gan";
UPDATE language_new SET language="赣语（简体）" WHERE iso_code="gan-hans";
UPDATE language_new SET language="贛語（繁體）" WHERE iso_code="gan-hant";
UPDATE language_new SET language="kriyòl gwiyannen" WHERE iso_code="gcr";
UPDATE language_new SET language="Gàidhlig" WHERE iso_code="gd";
UPDATE language_new SET language="galego" WHERE iso_code="gl";
UPDATE language_new SET language="на̄ни" WHERE iso_code="gld";
UPDATE language_new SET language="گیلکی" WHERE iso_code="glk";
UPDATE language_new SET language="Avañe'ẽ" WHERE iso_code="gn";
UPDATE language_new SET language="गोंयची कोंकणी / Gõychi Konknni" WHERE iso_code="gom";
UPDATE language_new SET language="गोंयची कोंकणी" WHERE iso_code="gom-deva";
UPDATE language_new SET language="Gõychi Konknni" WHERE iso_code="gom-latn";
UPDATE language_new SET language="Bahasa Hulontalo" WHERE iso_code="gor";
UPDATE language_new SET language="𐌲𐌿𐍄𐌹𐍃𐌺" WHERE iso_code="got";
UPDATE language_new SET language="Ghanaian Pidgin" WHERE iso_code="gpe";
UPDATE language_new SET language="Ἀρχαία ἑλληνικὴ" WHERE iso_code="grc";
UPDATE language_new SET language="Alemannisch" WHERE iso_code="gsw";
UPDATE language_new SET language="ગુજરાતી" WHERE iso_code="gu";
UPDATE language_new SET language="wayuunaiki" WHERE iso_code="guc";
UPDATE language_new SET language="farefare" WHERE iso_code="gur";
UPDATE language_new SET language="gungbe" WHERE iso_code="guw";
UPDATE language_new SET language="Gaelg" WHERE iso_code="gv";
UPDATE language_new SET language="Hausa" WHERE iso_code="ha";
UPDATE language_new SET language="客家語/Hak-kâ-ngî" WHERE iso_code="hak";
UPDATE language_new SET language="Hawaiʻi" WHERE iso_code="haw";
UPDATE language_new SET language="עברית" WHERE iso_code="he";
UPDATE language_new SET language="हिन्दी" WHERE iso_code="hi";
UPDATE language_new SET language="Fiji Hindi" WHERE iso_code="hif";
UPDATE language_new SET language="Fiji Hindi" WHERE iso_code="hif-latn";
UPDATE language_new SET language="Ilonggo" WHERE iso_code="hil";
UPDATE language_new SET language="Hiri Motu" WHERE iso_code="ho";
UPDATE language_new SET language="hrvatski" WHERE iso_code="hr";
UPDATE language_new SET language="Hunsrik" WHERE iso_code="hrx";
UPDATE language_new SET language="hornjoserbsce" WHERE iso_code="hsb";
UPDATE language_new SET language="湘语" WHERE iso_code="hsn";
UPDATE language_new SET language="Kreyòl ayisyen" WHERE iso_code="ht";
UPDATE language_new SET language="magyar" WHERE iso_code="hu";
UPDATE language_new SET language="magyar formal" WHERE iso_code="hu-formal";
UPDATE language_new SET language="հայերեն" WHERE iso_code="hy";
UPDATE language_new SET language="Արեւմտահայերէն" WHERE iso_code="hyw";
UPDATE language_new SET language="Otsiherero" WHERE iso_code="hz";
UPDATE language_new SET language="interlingua" WHERE iso_code="ia";
UPDATE language_new SET language="Bahasa Indonesia" WHERE iso_code="id";
UPDATE language_new SET language="Interlingue" WHERE iso_code="ie";
UPDATE language_new SET language="Igbo" WHERE iso_code="ig";
UPDATE language_new SET language="Igala" WHERE iso_code="igl";
UPDATE language_new SET language="ꆇꉙ" WHERE iso_code="ii";
UPDATE language_new SET language="Iñupiatun" WHERE iso_code="ik";
UPDATE language_new SET language="ᐃᓄᒃᑎᑐᑦ" WHERE iso_code="ike-cans";
UPDATE language_new SET language="inuktitut" WHERE iso_code="ike-latn";
UPDATE language_new SET language="Ilokano" WHERE iso_code="ilo";
UPDATE language_new SET language="гӀалгӀай" WHERE iso_code="inh";
UPDATE language_new SET language="Ido" WHERE iso_code="io";
UPDATE language_new SET language="íslenska" WHERE iso_code="is";
UPDATE language_new SET language="italiano" WHERE iso_code="it";
UPDATE language_new SET language="ᐃᓄᒃᑎᑐᑦ/inuktitut" WHERE iso_code="iu";
UPDATE language_new SET language="日本語" WHERE iso_code="ja";
UPDATE language_new SET language="Patois" WHERE iso_code="jam";
UPDATE language_new SET language="la .lojban." WHERE iso_code="jbo";
UPDATE language_new SET language="jysk" WHERE iso_code="jut";
UPDATE language_new SET language="Jawa" WHERE iso_code="jv";
UPDATE language_new SET language="ქართული" WHERE iso_code="ka";
UPDATE language_new SET language="Qaraqalpaqsha" WHERE iso_code="kaa";
UPDATE language_new SET language="Taqbaylit" WHERE iso_code="kab";
UPDATE language_new SET language="адыгэбзэ" WHERE iso_code="kbd";
UPDATE language_new SET language="адыгэбзэ" WHERE iso_code="kbd-cyrl";
UPDATE language_new SET language="Kabɩyɛ" WHERE iso_code="kbp";
UPDATE language_new SET language="Tyap" WHERE iso_code="kcg";
UPDATE language_new SET language="kabuverdianu" WHERE iso_code="kea";
UPDATE language_new SET language="Kongo" WHERE iso_code="kg";
UPDATE language_new SET language="کھوار" WHERE iso_code="khw";
UPDATE language_new SET language="Gĩkũyũ" WHERE iso_code="ki";
UPDATE language_new SET language="Kırmancki" WHERE iso_code="kiu";
UPDATE language_new SET language="Kwanyama" WHERE iso_code="kj";
UPDATE language_new SET language="хакас" WHERE iso_code="kjh";
UPDATE language_new SET language="ဖၠုံလိက်" WHERE iso_code="kjp";
UPDATE language_new SET language="қазақша" WHERE iso_code="kk";
UPDATE language_new SET language="قازاقشا تٴوتە" WHERE iso_code="kk-arab";
UPDATE language_new SET language="قازاقشا جۇنگو" WHERE iso_code="kk-cn";
UPDATE language_new SET language="қазақша кирил" WHERE iso_code="kk-cyrl";
UPDATE language_new SET language="қазақша Қазақстан" WHERE iso_code="kk-kz";
UPDATE language_new SET language="qazaqşa latın" WHERE iso_code="kk-latn";
UPDATE language_new SET language="qazaqşa Türkïya" WHERE iso_code="kk-tr";
UPDATE language_new SET language="kalaallisut" WHERE iso_code="kl";
UPDATE language_new SET language="ភាសាខ្មែរ" WHERE iso_code="km";
UPDATE language_new SET language="ಕನ್ನಡ" WHERE iso_code="kn";
UPDATE language_new SET language="한국어" WHERE iso_code="ko";
UPDATE language_new SET language="조선말" WHERE iso_code="ko-kp";
UPDATE language_new SET language="перем коми" WHERE iso_code="koi";
UPDATE language_new SET language="kanuri" WHERE iso_code="kr";
UPDATE language_new SET language="къарачай-малкъар" WHERE iso_code="krc";
UPDATE language_new SET language="Krio" WHERE iso_code="kri";
UPDATE language_new SET language="Kinaray-a" WHERE iso_code="krj";
UPDATE language_new SET language="karjal" WHERE iso_code="krl";
UPDATE language_new SET language="कॉशुर / کٲشُر" WHERE iso_code="ks";
UPDATE language_new SET language="کٲشُر" WHERE iso_code="ks-arab";
UPDATE language_new SET language="कॉशुर" WHERE iso_code="ks-deva";
UPDATE language_new SET language="Ripoarisch" WHERE iso_code="ksh";
UPDATE language_new SET language="စှီၤ" WHERE iso_code="ksw";
UPDATE language_new SET language="kurdî" WHERE iso_code="ku";
UPDATE language_new SET language="كوردي عەرەبی" WHERE iso_code="ku-arab";
UPDATE language_new SET language="kurdî latînî" WHERE iso_code="ku-latn";
UPDATE language_new SET language="къумукъ" WHERE iso_code="kum";
UPDATE language_new SET language="коми" WHERE iso_code="kv";
UPDATE language_new SET language="kernowek" WHERE iso_code="kw";
UPDATE language_new SET language="кыргызча" WHERE iso_code="ky";
UPDATE language_new SET language="Latina" WHERE iso_code="la";
UPDATE language_new SET language="Ladino" WHERE iso_code="lad";
UPDATE language_new SET language="Lëtzebuergesch" WHERE iso_code="lb";
UPDATE language_new SET language="лакку" WHERE iso_code="lbe";
UPDATE language_new SET language="лезги" WHERE iso_code="lez";
UPDATE language_new SET language="Lingua Franca Nova" WHERE iso_code="lfn";
UPDATE language_new SET language="Luganda" WHERE iso_code="lg";
UPDATE language_new SET language="Limburgs" WHERE iso_code="li";
UPDATE language_new SET language="Ligure" WHERE iso_code="lij";
UPDATE language_new SET language="Līvõ kēļ" WHERE iso_code="liv";
UPDATE language_new SET language="لەکی" WHERE iso_code="lki";
UPDATE language_new SET language="Ladin" WHERE iso_code="lld";
UPDATE language_new SET language="lombard" WHERE iso_code="lmo";
UPDATE language_new SET language="lingála" WHERE iso_code="ln";
UPDATE language_new SET language="ລາວ" WHERE iso_code="lo";
UPDATE language_new SET language="Silozi" WHERE iso_code="loz";
UPDATE language_new SET language="لۊری شومالی" WHERE iso_code="lrc";
UPDATE language_new SET language="lietuvių" WHERE iso_code="lt";
UPDATE language_new SET language="latgaļu" WHERE iso_code="ltg";
UPDATE language_new SET language="Mizo ţawng" WHERE iso_code="lus";
UPDATE language_new SET language="لئری دوٙمینی" WHERE iso_code="luz";
UPDATE language_new SET language="latviešu" WHERE iso_code="lv";
UPDATE language_new SET language="文言" WHERE iso_code="lzh";
UPDATE language_new SET language="Lazuri" WHERE iso_code="lzz";
UPDATE language_new SET language="Madhurâ" WHERE iso_code="mad";
UPDATE language_new SET language="मगही" WHERE iso_code="mag";
UPDATE language_new SET language="मैथिली" WHERE iso_code="mai";
UPDATE language_new SET language="Basa Banyumasan" WHERE iso_code="map-bms";
UPDATE language_new SET language="мокшень" WHERE iso_code="mdf";
UPDATE language_new SET language="Malagasy" WHERE iso_code="mg";
UPDATE language_new SET language="Ebon" WHERE iso_code="mh";
UPDATE language_new SET language="олык марий" WHERE iso_code="mhr";
UPDATE language_new SET language="Māori" WHERE iso_code="mi";
UPDATE language_new SET language="Minangkabau" WHERE iso_code="min";
UPDATE language_new SET language="македонски" WHERE iso_code="mk";
UPDATE language_new SET language="മലയാളം" WHERE iso_code="ml";
UPDATE language_new SET language="монгол" WHERE iso_code="mn";
UPDATE language_new SET language="ꯃꯤꯇꯩ ꯂꯣꯟ" WHERE iso_code="mni";
UPDATE language_new SET language="ဘာသာ မန်" WHERE iso_code="mnw";
UPDATE language_new SET language="молдовеняскэ" WHERE iso_code="mo";
UPDATE language_new SET language="moore" WHERE iso_code="mos";
UPDATE language_new SET language="मराठी" WHERE iso_code="mr";
UPDATE language_new SET language="Mara" WHERE iso_code="mrh";
UPDATE language_new SET language="кырык мары" WHERE iso_code="mrj";
UPDATE language_new SET language="Bahasa Melayu" WHERE iso_code="ms";
UPDATE language_new SET language="بهاس ملايو" WHERE iso_code="ms-arab";
UPDATE language_new SET language="Malti" WHERE iso_code="mt";
UPDATE language_new SET language="Mvskoke" WHERE iso_code="mus";
UPDATE language_new SET language="Mirandés" WHERE iso_code="mwl";
UPDATE language_new SET language="မြန်မာဘာသာ" WHERE iso_code="my";
UPDATE language_new SET language="эрзянь" WHERE iso_code="myv";
UPDATE language_new SET language="مازِرونی" WHERE iso_code="mzn";
UPDATE language_new SET language="Dorerin Naoero" WHERE iso_code="na";
UPDATE language_new SET language="Nāhuatl" WHERE iso_code="nah";
UPDATE language_new SET language="Bân-lâm-gú" WHERE iso_code="nan";
UPDATE language_new SET language="Napulitano" WHERE iso_code="nap";
UPDATE language_new SET language="norsk bokmål" WHERE iso_code="nb";
UPDATE language_new SET language="Plattdüütsch" WHERE iso_code="nds";
UPDATE language_new SET language="Nedersaksies" WHERE iso_code="nds-nl";
UPDATE language_new SET language="नेपाली" WHERE iso_code="ne";
UPDATE language_new SET language="नेपाल भाषा" WHERE iso_code="new";
UPDATE language_new SET language="Oshiwambo" WHERE iso_code="ng";
UPDATE language_new SET language="Li Niha" WHERE iso_code="nia";
UPDATE language_new SET language="Niuē" WHERE iso_code="niu";
UPDATE language_new SET language="Nederlands" WHERE iso_code="nl";
UPDATE language_new SET language="Nederlands informeel" WHERE iso_code="nl-informal";
UPDATE language_new SET language="nawdm" WHERE iso_code="nmz";
UPDATE language_new SET language="norsk nynorsk" WHERE iso_code="nn";
UPDATE language_new SET language="norsk" WHERE iso_code="no";
UPDATE language_new SET language="ᨣᩤᩴᨾᩮᩬᩥᨦ" WHERE iso_code="nod";
UPDATE language_new SET language="Novial" WHERE iso_code="nov";
UPDATE language_new SET language="ߒߞߏ" WHERE iso_code="nqo";
UPDATE language_new SET language="Nouormand" WHERE iso_code="nrm";
UPDATE language_new SET language="Sesotho sa Leboa" WHERE iso_code="nso";
UPDATE language_new SET language="Diné bizaad" WHERE iso_code="nv";
UPDATE language_new SET language="Chi-Chewa" WHERE iso_code="ny";
UPDATE language_new SET language="runyankore" WHERE iso_code="nyn";
UPDATE language_new SET language="Nyunga" WHERE iso_code="nys";
UPDATE language_new SET language="occitan" WHERE iso_code="oc";
UPDATE language_new SET language="Ojibwemowin" WHERE iso_code="ojb";
UPDATE language_new SET language="livvinkarjala" WHERE iso_code="olo";
UPDATE language_new SET language="Oromoo" WHERE iso_code="om";
UPDATE language_new SET language="ଓଡ଼ିଆ" WHERE iso_code="or";
UPDATE language_new SET language="ирон" WHERE iso_code="os";
UPDATE language_new SET language="ਪੰਜਾਬੀ" WHERE iso_code="pa";
UPDATE language_new SET language="Pangasinan" WHERE iso_code="pag";
UPDATE language_new SET language="Kapampangan" WHERE iso_code="pam";
UPDATE language_new SET language="Papiamentu" WHERE iso_code="pap";
UPDATE language_new SET language="Picard" WHERE iso_code="pcd";
UPDATE language_new SET language="Naijá" WHERE iso_code="pcm";
UPDATE language_new SET language="Deitsch" WHERE iso_code="pdc";
UPDATE language_new SET language="Plautdietsch" WHERE iso_code="pdt";
UPDATE language_new SET language="Pälzisch" WHERE iso_code="pfl";
UPDATE language_new SET language="पालि" WHERE iso_code="pi";
UPDATE language_new SET language="Norfuk / Pitkern" WHERE iso_code="pih";
UPDATE language_new SET language="polski" WHERE iso_code="pl";
UPDATE language_new SET language="Piemontèis" WHERE iso_code="pms";
UPDATE language_new SET language="پنجابی" WHERE iso_code="pnb";
UPDATE language_new SET language="Ποντιακά" WHERE iso_code="pnt";
UPDATE language_new SET language="prūsiskan" WHERE iso_code="prg";
UPDATE language_new SET language="پښتو" WHERE iso_code="ps";
UPDATE language_new SET language="português" WHERE iso_code="pt";
UPDATE language_new SET language="português do Brasil" WHERE iso_code="pt-br";
UPDATE language_new SET language="pinayuanan" WHERE iso_code="pwn";
UPDATE language_new SET language="Runa Simi" WHERE iso_code="qu";
UPDATE language_new SET language="Runa shimi" WHERE iso_code="qug";
UPDATE language_new SET language="Rumagnôl" WHERE iso_code="rgn";
UPDATE language_new SET language="Tarifit" WHERE iso_code="rif";
UPDATE language_new SET language="ရခိုင်" WHERE iso_code="rki";
UPDATE language_new SET language="rumantsch" WHERE iso_code="rm";
UPDATE language_new SET language="romaňi čhib" WHERE iso_code="rmc";
UPDATE language_new SET language="romani čhib" WHERE iso_code="rmy";
UPDATE language_new SET language="ikirundi" WHERE iso_code="rn";
UPDATE language_new SET language="română" WHERE iso_code="ro";
UPDATE language_new SET language="armãneashti" WHERE iso_code="roa-rup";
UPDATE language_new SET language="tarandíne" WHERE iso_code="roa-tara";
UPDATE language_new SET language="руски" WHERE iso_code="rsk";
UPDATE language_new SET language="русский" WHERE iso_code="ru";
UPDATE language_new SET language="русиньскый" WHERE iso_code="rue";
UPDATE language_new SET language="armãneashti" WHERE iso_code="rup";
UPDATE language_new SET language="Vlăheşte" WHERE iso_code="ruq";
UPDATE language_new SET language="Влахесте" WHERE iso_code="ruq-cyrl";
UPDATE language_new SET language="Vlăheşte" WHERE iso_code="ruq-latn";
UPDATE language_new SET language="Ikinyarwanda" WHERE iso_code="rw";
UPDATE language_new SET language="うちなーぐち" WHERE iso_code="ryu";
UPDATE language_new SET language="संस्कृतम्" WHERE iso_code="sa";
UPDATE language_new SET language="саха тыла" WHERE iso_code="sah";
UPDATE language_new SET language="ᱥᱟᱱᱛᱟᱲᱤ" WHERE iso_code="sat";
UPDATE language_new SET language="sardu" WHERE iso_code="sc";
UPDATE language_new SET language="sicilianu" WHERE iso_code="scn";
UPDATE language_new SET language="Scots" WHERE iso_code="sco";
UPDATE language_new SET language="سنڌي" WHERE iso_code="sd";
UPDATE language_new SET language="Sassaresu" WHERE iso_code="sdc";
UPDATE language_new SET language="کوردی خوارگ" WHERE iso_code="sdh";
UPDATE language_new SET language="davvisámegiella" WHERE iso_code="se";
UPDATE language_new SET language="davvisámegiella Suoma bealde" WHERE iso_code="se-fi";
UPDATE language_new SET language="davvisámegiella Norgga bealde" WHERE iso_code="se-no";
UPDATE language_new SET language="davvisámegiella Ruoŧa bealde" WHERE iso_code="se-se";
UPDATE language_new SET language="Cmique Itom" WHERE iso_code="sei";
UPDATE language_new SET language="Koyraboro Senni" WHERE iso_code="ses";
UPDATE language_new SET language="Sängö" WHERE iso_code="sg";
UPDATE language_new SET language="žemaitėška" WHERE iso_code="sgs";
UPDATE language_new SET language="srpskohrvatski / српскохрватски" WHERE iso_code="sh";
UPDATE language_new SET language="српскохрватски ћирилица" WHERE iso_code="sh-cyrl";
UPDATE language_new SET language="srpskohrvatski latinica" WHERE iso_code="sh-latn";
UPDATE language_new SET language="Taclḥit" WHERE iso_code="shi";
UPDATE language_new SET language="Taclḥit" WHERE iso_code="shi-latn";
UPDATE language_new SET language="ⵜⴰⵛⵍⵃⵉⵜ" WHERE iso_code="shi-tfng";
UPDATE language_new SET language="ၽႃႇသႃႇတႆး " WHERE iso_code="shn";
UPDATE language_new SET language="tacawit" WHERE iso_code="shy";
UPDATE language_new SET language="tacawit" WHERE iso_code="shy-latn";
UPDATE language_new SET language="සිංහල" WHERE iso_code="si";
UPDATE language_new SET language="Simple English" WHERE iso_code="simple";
UPDATE language_new SET language="кӣллт са̄мь кӣлл" WHERE iso_code="sjd";
UPDATE language_new SET language="bidumsámegiella" WHERE iso_code="sje";
UPDATE language_new SET language="slovenčina" WHERE iso_code="sk";
UPDATE language_new SET language="سرائیکی" WHERE iso_code="skr";
UPDATE language_new SET language="سرائیکی" WHERE iso_code="skr-arab";
UPDATE language_new SET language="slovenščina" WHERE iso_code="sl";
UPDATE language_new SET language="Schläsch" WHERE iso_code="sli";
UPDATE language_new SET language="Gagana Samoa" WHERE iso_code="sm";
UPDATE language_new SET language="åarjelsaemien" WHERE iso_code="sma";
UPDATE language_new SET language="anarâškielâ" WHERE iso_code="smn";
UPDATE language_new SET language="nuõrttsääʹmǩiõll" WHERE iso_code="sms";
UPDATE language_new SET language="chiShona" WHERE iso_code="sn";
UPDATE language_new SET language="Soomaaliga" WHERE iso_code="so";
UPDATE language_new SET language="shqip" WHERE iso_code="sq";
UPDATE language_new SET language="српски / srpski" WHERE iso_code="sr";
UPDATE language_new SET language="српски ћирилица" WHERE iso_code="sr-ec";
UPDATE language_new SET language="srpski latinica" WHERE iso_code="sr-el";
UPDATE language_new SET language="Sranantongo" WHERE iso_code="srn";
UPDATE language_new SET language="sardu campidanesu" WHERE iso_code="sro";
UPDATE language_new SET language="SiSwati" WHERE iso_code="ss";
UPDATE language_new SET language="Sesotho" WHERE iso_code="st";
UPDATE language_new SET language="Seeltersk" WHERE iso_code="stq";
UPDATE language_new SET language="себертатар" WHERE iso_code="sty";
UPDATE language_new SET language="Sunda" WHERE iso_code="su";
UPDATE language_new SET language="svenska" WHERE iso_code="sv";
UPDATE language_new SET language="Kiswahili" WHERE iso_code="sw";
UPDATE language_new SET language="ꠍꠤꠟꠐꠤ" WHERE iso_code="syl";
UPDATE language_new SET language="ślůnski" WHERE iso_code="szl";
UPDATE language_new SET language="Sakizaya" WHERE iso_code="szy";
UPDATE language_new SET language="தமிழ்" WHERE iso_code="ta";
UPDATE language_new SET language="Tayal" WHERE iso_code="tay";
UPDATE language_new SET language="ತುಳು" WHERE iso_code="tcy";
UPDATE language_new SET language="ᥖᥭᥰᥖᥬᥳᥑᥨᥒᥰ" WHERE iso_code="tdd";
UPDATE language_new SET language="తెలుగు" WHERE iso_code="te";
UPDATE language_new SET language="tetun" WHERE iso_code="tet";
UPDATE language_new SET language="тоҷикӣ" WHERE iso_code="tg";
UPDATE language_new SET language="тоҷикӣ" WHERE iso_code="tg-cyrl";
UPDATE language_new SET language="tojikī" WHERE iso_code="tg-latn";
UPDATE language_new SET language="ไทย" WHERE iso_code="th";
UPDATE language_new SET language="ትግርኛ" WHERE iso_code="ti";
UPDATE language_new SET language="Türkmençe" WHERE iso_code="tk";
UPDATE language_new SET language="Tagalog" WHERE iso_code="tl";
UPDATE language_new SET language="tolışi" WHERE iso_code="tly";
UPDATE language_new SET language="толыши" WHERE iso_code="tly-cyrl";
UPDATE language_new SET language="Setswana" WHERE iso_code="tn";
UPDATE language_new SET language="lea faka-Tonga" WHERE iso_code="to";
UPDATE language_new SET language="toki pona" WHERE iso_code="tok";
UPDATE language_new SET language="Tok Pisin" WHERE iso_code="tpi";
UPDATE language_new SET language="Türkçe" WHERE iso_code="tr";
UPDATE language_new SET language="Ṫuroyo" WHERE iso_code="tru";
UPDATE language_new SET language="Seediq" WHERE iso_code="trv";
UPDATE language_new SET language="Xitsonga" WHERE iso_code="ts";
UPDATE language_new SET language="татарча/tatarça" WHERE iso_code="tt";
UPDATE language_new SET language="татарча" WHERE iso_code="tt-cyrl";
UPDATE language_new SET language="tatarça" WHERE iso_code="tt-latn";
UPDATE language_new SET language="chiTumbuka" WHERE iso_code="tum";
UPDATE language_new SET language="Twi" WHERE iso_code="tw";
UPDATE language_new SET language="reo tahiti" WHERE iso_code="ty";
UPDATE language_new SET language="тыва дыл" WHERE iso_code="tyv";
UPDATE language_new SET language="ⵜⴰⵎⴰⵣⵉⵖⵜ" WHERE iso_code="tzm";
UPDATE language_new SET language="удмурт" WHERE iso_code="udm";
UPDATE language_new SET language="ئۇيغۇرچە / Uyghurche" WHERE iso_code="ug";
UPDATE language_new SET language="ئۇيغۇرچە" WHERE iso_code="ug-arab";
UPDATE language_new SET language="Uyghurche" WHERE iso_code="ug-latn";
UPDATE language_new SET language="українська" WHERE iso_code="uk";
UPDATE language_new SET language="اردو" WHERE iso_code="ur";
UPDATE language_new SET language="oʻzbekcha/ўзбекча" WHERE iso_code="uz";
UPDATE language_new SET language="ўзбекча" WHERE iso_code="uz-cyrl";
UPDATE language_new SET language="oʻzbekcha" WHERE iso_code="uz-latn";
UPDATE language_new SET language="Tshivenda" WHERE iso_code="ve";
UPDATE language_new SET language="vèneto" WHERE iso_code="vec";
UPDATE language_new SET language="vepsän kel’" WHERE iso_code="vep";
UPDATE language_new SET language="Tiếng Việt" WHERE iso_code="vi";
UPDATE language_new SET language="West-Vlams" WHERE iso_code="vls";
UPDATE language_new SET language="Mainfränkisch" WHERE iso_code="vmf";
UPDATE language_new SET language="emakhuwa" WHERE iso_code="vmw";
UPDATE language_new SET language="Volapük" WHERE iso_code="vo";
UPDATE language_new SET language="Vaďďa" WHERE iso_code="vot";
UPDATE language_new SET language="võro" WHERE iso_code="vro";
UPDATE language_new SET language="walon" WHERE iso_code="wa";
UPDATE language_new SET language="Winaray" WHERE iso_code="war";
UPDATE language_new SET language="Fakaʻuvea" WHERE iso_code="wls";
UPDATE language_new SET language="Wolof" WHERE iso_code="wo";
UPDATE language_new SET language="吴语" WHERE iso_code="wuu";
UPDATE language_new SET language="хальмг" WHERE iso_code="xal";
UPDATE language_new SET language="isiXhosa" WHERE iso_code="xh";
UPDATE language_new SET language="მარგალური" WHERE iso_code="xmf";
UPDATE language_new SET language="saisiyat" WHERE iso_code="xsy";
UPDATE language_new SET language="ייִדיש" WHERE iso_code="yi";
UPDATE language_new SET language="Yorùbá" WHERE iso_code="yo";
UPDATE language_new SET language="Nhẽẽgatú" WHERE iso_code="yrl";
UPDATE language_new SET language="粵語" WHERE iso_code="yue";
UPDATE language_new SET language="Vahcuengh" WHERE iso_code="za";
UPDATE language_new SET language="Zeêuws" WHERE iso_code="zea";
UPDATE language_new SET language="ⵜⴰⵎⴰⵣⵉⵖⵜ ⵜⴰⵏⴰⵡⴰⵢⵜ" WHERE iso_code="zgh";
UPDATE language_new SET language="中文" WHERE iso_code="zh";
UPDATE language_new SET language="文言" WHERE iso_code="zh-classical";
UPDATE language_new SET language="中文（中国大陆）" WHERE iso_code="zh-cn";
UPDATE language_new SET language="中文（简体）" WHERE iso_code="zh-hans";
UPDATE language_new SET language="中文（繁體）" WHERE iso_code="zh-hant";
UPDATE language_new SET language="中文（香港）" WHERE iso_code="zh-hk";
UPDATE language_new SET language="Bân-lâm-gú" WHERE iso_code="zh-min-nan";
UPDATE language_new SET language="中文（澳門）" WHERE iso_code="zh-mo";
UPDATE language_new SET language="中文（马来西亚）" WHERE iso_code="zh-my";
UPDATE language_new SET language="中文（新加坡）" WHERE iso_code="zh-sg";
UPDATE language_new SET language="中文（臺灣）" WHERE iso_code="zh-tw";
UPDATE language_new SET language="粵語" WHERE iso_code="zh-yue";
UPDATE language_new SET language="isiZulu" WHERE iso_code="zu";
UPDATE language_new SET language="other" WHERE iso_code="other";

# swap tables
RENAME TABLE language TO language_old;
RENAME TABLE language_new TO language;

#########################################################
# MIGRATE PROJECT TABLE TO UTF8MB4, PURGING JUNK VALUES #
#########################################################

CREATE TABLE project_new (
  id smallint unsigned NOT NULL AUTO_INCREMENT,
  project varchar(128) NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY project (project)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

# remove one project that is clearly garbage
# ..\..\..\..\..\..\..\..\..\..\..\..\..\..\..\..\winnt\win.ini
DELETE FROM bannerimpressions WHERE project_id = 745;

# create a list of active project ids
CREATE TEMPORARY TABLE project_ids (id INT(11) DEFAULT NULL, UNIQUE KEY id (id));
REPLACE INTO project_ids SELECT DISTINCT(project_id) FROM bannerimpression_raw;
REPLACE INTO project_ids SELECT DISTINCT(project_id) FROM bannerimpressions;
REPLACE INTO project_ids SELECT DISTINCT(project_id) FROM landingpageimpression_raw;
REPLACE INTO project_ids SELECT DISTINCT(project_id) FROM landingpageimpressions;

# import rows for active projects
INSERT INTO project_new SELECT * FROM project WHERE id IN (SELECT id FROM project_ids);

# swap tables
RENAME TABLE project TO project_old;
RENAME TABLE project_new TO project;
