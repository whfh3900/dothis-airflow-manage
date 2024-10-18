import re
import pandas as pd
from tqdm import tqdm
tqdm.pandas()
from transformers import AutoTokenizer, AutoModel
import ast
from collections import OrderedDict
import time
import torch
from gliner import GLiNER
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import json

# 아스키코드 치환
def ascii_check(word):
    word_type = type(word)
    if word_type is not list:
        word = str(word).split()
    result = list()
    for text in word:
        text = list(text)
        for i in range(len(text)):
            if ord(text[i]) > 55203:
                ascii_num = ord(text[i])-65248
                try:
                    text[i] = chr(ascii_num)
                except ValueError as e:
                    text[i] = ' '
        result.append(''.join(text))
    if word_type is not list:
        return ' '.join(result)
    else:
        return result
    
    
# 정규표현식 패턴을 사용하여 특수문자 제거
def remove_special_characters(text, use_token=None, use_hashtag=False):
    try:
        cleaned_list = re.sub(r'[^a-zA-Z0-9가-힣\s]', '', text).split()
    except Exception as e:
        return ""
    if use_token:
        cleaned_text = " ".join(cleaned_list[use_token:])
    else:
        cleaned_text =  " ".join(cleaned_list)
    if use_hashtag:
        cleaned_text = "#"+" #".join(cleaned_text.split())
    return cleaned_text


# 해쉬태그 추출
def hashtag_extraction(text):

    if isinstance(text, str):
        pattern = '#([0-9a-zA-Z가-힣]*)'
        hash_w = re.compile(pattern)
        hash_tag = ["#"+hash for hash in hash_w.findall(text)]
        return ' '.join(hash_tag)
    else:
        return ''
    
# 뉴스api 데이터 전처리
def news_remove_words(text):
    # 괄호 안에 있는 단어 삭제
    text = re.sub(r'\([^)]*\)', '', text)  # () 안에 있는 모든 문자열 삭제
    text = re.sub(r'\[[^\]]*\]', '', text)  # [] 안에 있는 모든 문자열 삭제
    text = re.sub(r'\【[^\]]*\】', '', text)  # 【】 안에 있는 모든 문자열 삭제
    text = text.replace("<b>", "")
    text = text.replace("</b>", "")
    text = text.replace("\xa0", " ")
    return text
    
# 한글여부 판독기
def calculate_korean_ratio(text, ratio=.0):
    pattern = '([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)' # E-mail제거
    text = re.sub(pattern=pattern, repl='', string=text)
    pattern = '(http|ftp|https)://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+' # URL제거
    text = re.sub(pattern=pattern, repl=' ', string=text)
    clean_text = re.sub(r"[^\w\s]", " ", text) # 특수문자 제거
    words = clean_text.split()
    # total_words = len(words)  # 전체 단어 개수    
    korean_words = 0  # 한글 단어 개수

    for word in words:
        for char in word:
            if '\uAC00' <= char <= '\uD7A3':
                korean_words += 1
                # 한국어가 하나라도 포함되면 True
                # break
                return True
    return False


# 영단어 문자 확인 함수
def is_english_word(text):
    pattern = r'^[a-zA-Z]+$'  # 알파벳 대소문자로만 구성된 문자열인지 확인하기 위한 정규표현식
    return bool(re.match(pattern, text))

# 한글 단어 확인 함수
def is_korea_word(text):
    pattern = r"^[ㄱ-ㅎㅏ-ㅣ가-힣]+$"
    return bool(re.match(pattern, text))

# mecab 안쓰고 태그단어 추출
def just_tag_extract(text):
    # 문자열을 리스트로 변환
    try:
        lst = ast.literal_eval(text)
    except ValueError as e:
        return ""
    # 리스트의 각 요소 앞에 '#' 추가
    # 길이가 1이 아닐 경우에만
    result = ' '.join(['#' + item for item in lst if len(item) != 1])
    return result


##########################################################################
# 2개의 문장 유사도 계산
def analyzing_word_frequency(text1, text2):
    text1_words = set(text1.split())
    text2_words = set(text2.split())
    combination_words = text1_words|text2_words
    intersection_words = text1_words&text2_words
    if len(intersection_words) == 0:
        return 0
    else:
        return len(intersection_words)/len(combination_words)


# 유사한 것은 빼도록 하는 코드
# 채널별별로 전처리한 텍스트를 dict 형태로 만들고 하나씩 비교해서 유사도가 높은것은 제외
# 채널의 영상 평균값을 임계값으로 하여 마지막 threshold개의 영상만 사용하자
def duplicate_text_extract(dict, threshold):
    texts = set()
    for key, v1 in dict.items():
        for v2 in list(dict.values())[key+1:]:
            awf = analyzing_word_frequency(v1, v2)
            if awf < 0.3:
                texts.add(v1)
    texts = list(texts)
    if len(texts) > threshold:
        return texts[len(texts)-threshold:]
    else:
        return texts
##########################################################################

def has_number_and_special_character(text):
    pattern = r'[^a-zA-Z가-힣\s]'  # 특수문자 패턴
    return  bool(re.search(pattern, text))

### Mecab 업데이트 전 조사삭제를 위한 함수
def custom_endswith_exclude(word, suffix_list, exclude_list):
    return any(word.endswith(suffix) and not word.endswith(tuple(exclude_list)) for suffix in suffix_list)


# 문장별로 중복되는 토큰은 제거
def remove_duplicate_tokens(text, use_reversed=False):
    if use_reversed:
        new_list = list(OrderedDict.fromkeys(reversed(text.split())))
    else:
        new_list = list(OrderedDict.fromkeys(text.split()))
    return " ".join(new_list)

# 이모티콘 삭제
def remove_emojis(text):
    # 정규 표현식 패턴으로 이모티콘 찾기
    emoji_pattern = re.compile("["
                               u"\U0001F600-\U0001F64F"  # 스마일 이모티콘
                               u"\U0001F300-\U0001F5FF"  # 기호 및 기타 심볼
                               u"\U0001F680-\U0001F6FF"  # 기타 이모티콘
                               u"\U0001F1E0-\U0001F1FF"  # 국기 이모티콘
                               "]+", flags=re.UNICODE)
    # 이모티콘을 공백으로 대체
    return emoji_pattern.sub(r' ', text)

# 시간 계산 함수
def calculate_execution_time(func, *args, **kwargs):
    start_time = time.time()
    result = func(*args, **kwargs)
    end_time = time.time()
    execution_time = end_time - start_time

    # 시간 단위 변환
    hours, remainder = divmod(execution_time, 3600)
    minutes, seconds = divmod(remainder, 60)

    print(f"Execution time: {int(hours)}시간 {int(minutes)}분 {seconds:.2f}초")
    return result

### 한국어나 외국어로 시작하는지 검사하는 함수
### 숫자나 특수문자로 시작하는 경우에는 삭제하는 경우에 사용
def starts_with_korean_or_english(word):
    # 정규표현식 패턴
    pattern = r'^[가-힣a-zA-Z]'
    # 패턴에 맞는지 확인하여 결과 반환
    return bool(re.match(pattern, word))

### 한국어 검사하는 함수
def is_korean(text):
    for char in text:
        if not ('\uac00' <= char <= '\ud7a3' or '\u1100' <= char <= '\u11ff' or '\u3130' <= char <= '\u318f'):
            return False
    return True

def decode_and_convert(s):
    # 바이트 문자열이면 UTF-8로 디코딩
    if isinstance(s, bytes):
        try:
            s = s.decode('utf-8')
            s = json.loads(s)
        except Exception as e:
            print(f"Error decoding bytes: {e}")
            return s
    
    if isinstance(s, str):
        # 문자열을 리스트로 변환
        try:
            list_data = ast.literal_eval(s)
        except Exception as e:
            print(f"Error parsing {s}: {e}")
            return s
        # 리스트 내의 유니코드 이스케이프 시퀀스를 변환
        return [elem.encode('latin1').decode('unicode_escape') if isinstance(elem, str) else elem for elem in list_data]
    
    return s


def clean_and_parse_list(x):
    if isinstance(x, str):
        # 정규표현식을 사용해 문자열에서 불필요한 따옴표와 공백 제거
        cleaned = re.sub(r"['\"\s]+", '', x)
        
        # 문자열이 대괄호로 시작하고 끝나면 이를 쉼표로 구분된 리스트로 변환
        if cleaned.startswith('[') and cleaned.endswith(']'):
            # 대괄호 안의 내용물을 쉼표로 나누고 리스트로 변환
            return cleaned[1:-1].split(',')
        else:
            return [cleaned]  # 리스트가 아니더라도 단일 항목으로 리스트 반환
    return []  # None이나 다른 타입인 경우 빈 리스트 반환


def is_two_char_with_english(text):
    # 문자열이 2글자이고, 영문자가 포함된 경우 True 반환
    return len(text) == 2 and bool(re.search(r'[A-Za-z]', text))


class PreProcessing():
    def __init__(self, model="taeminlee/gliner_ko", 
                 tta_labels = None,
                 cache_dir = "../../models/huggingface/",
                 stopwords_path = "../../data/stopwords/stopwords_fin.txt",
                 use_cuda=True):
        self.model = GLiNER.from_pretrained(model, cache_dir=cache_dir)
    
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        if use_cuda:
            if self.device == "cuda":
                # 모델을 CUDA로 이동
                self.model.to(self.device)
            else:
                print("cuda를 지원하지 않습니다.")
                
        if tta_labels is None:
            self.tta_labels = ["artifacts", "person", "animal", "CIVILIZATION", "organization", "Activity", \
                    "phone number", "address", "passport number", "email", "credit card number", "Album", \
                    "social security number", "health insurance id number", "date of birth", "Analysis Requirement", \
                    "mobile phone number", "bank account number", "medication", "cpf", "driver's license number", \
                    "tax identification number", "medical condition", "identity card number", "national id number", \
                    "ip address", "email address", "iban", "credit card expiration date", "username", "Anatomy", \
                    "health insurance number", "registration number", "student id number", "insurance number", \
                    "flight number", "landline phone number", "blood type", "cvv", "reservation number", \
                    "digital signature", "social media handle", "license plate number", "cnpj", "postal code", \
                    "passport_number", "serial number", "vehicle registration number", "credit card brand", \
                    "fax number", "visa number", "insurance company", "identity document number", "transaction number", \
                    "national health insurance number", "cvc", "birth certificate number", "train ticket number", \
                    "passport expiration date", "social_security_number", "EVENT", "STUDY_FIELD", "LOCATION", \
                    "MATERIAL", "PLANT", "QUANTITY", "TERM", "THEORY", "Accommodation", "Action Item", 'activity/event',\
                    'Event Venue', 'product/service', 'Artwork', 'musical instrument', 'material', 'card name', \
                    'phone number', 'Document', 'Social Issue', 'Event Reservation', 'experience', 'City', 'Tone', \
                    'transportation', 'Action Item', 'Phone Number Component', 'Time Frame', 'Religious Place', \
                    'Award', 'Furniture', 'Plant species', 'Food', 'Phone Number', 'database', 'time period', \
                    'Sample Submission', 'Product', 'Medical condition', 'group/person', 'mental/physical strain', \
                    'Issue/problem', 'Analysis Requirement', 'Device', 'person/artist name', 'Kitchen', 'Brand', \
                    'Event/Program', 'Hairstyle', 'Time/Period', 'Document component', 'writing skills', 'Building', \
                    'resource', 'Film Title', 'Material Type', 'measurement', 'Media', 'Telecommunications company', \
                    'Appliance', 'insect species', 'medical condition', 'Time/Date', 'communication method', 'action', \
                    'tool/process', 'person', 'Organization', 'Year', 'Retailer', 'tool', 'Payment', 'Printing paper', \
                    'Medical device', 'Country', 'event', 'Date', 'Religious Ceremony', 'Idiom', 'Restaurant/Cafe', \
                    'geographic location', 'Location', 'File type', 'demographic group', 'Printing technique', \
                    'technology', 'Business/organization', 'Cricket Team', 'Accommodation', 'artwork title', 'Software', \
                    'Job title', 'Province', 'Company', 'Emotion', 'Industry', 'Publisher', 'education', \
                    'website', 'Color', 'navigation', 'device', 'Cricketer', 'writing techniques', 'Political entity', \
                    'location', 'holiday/event', 'target audience', 'Landmark', 'product', 'Person', 'Event',
                    'Batting Score', 'group', 'geographical location', 'object', 'Hardware','Educational institution', \
                    'action/event', 'programme name', 'Time', 'Anatomy', 'Religious Leader', 'physical attribute', \
                    'Fashion Brand', 'person/group', 'Holiday/Event', 'Activity', 'Bag Type', 'brand/logo', 'inquiry type', \
                    'kitchen utensil', 'Ingredient', 'Album', 'Procedure', 'Global Issue', 'Technology Platform', \
                    'Award Category', 'Fashion Item', 'Facility', 'website users', 'Gender', 'occupation', 'group of people', \
                    'job requirements', 'transportation service', 'artistic creation', 'Official document', 'electronic device'
                    ]
        else:
            if isinstance(tta_labels, list):
                self.tta_labels = tta_labels
            else:
                raise TypeError("The 'tta_labels' must be provided in list format. %s" %tta_labels) from None
                    
        self.stopwords = set()
        with open(stopwords_path, "r", encoding="utf-8-sig") as f:
            while True:
                text = f.readline().strip()
                if (text != ""):
                    self.stopwords.add(text)
                if not text:
                    break
        self.stopwords = list(self.stopwords)

    def use_norns(self, text, use_upper=True, use_stopword=True, num_len=10, threshold=0.1):
        if isinstance(text, str):
            
            ## UserWarning: Sentence of length 416 has been truncated to 384 warnings.warn(f"Sentence of length {len(tokens)} has been truncated to {max_len}")
            text = remove_emojis(text.encode('utf-8', 'ignore').decode('utf-8')).strip()
            if len(text) > 384:
                truncated_text = text[:384]
                if text[384] != ' ':
                    # If the last character is not a space, find the last space and truncate to that point
                    text = truncated_text.rsplit(' ', 1)[0]
                else:
                    text = truncated_text
            
            try:
                entities = self.model.predict_entities(text, self.tta_labels, threshold=threshold)
                
                if use_upper and use_stopword:
                    return [(entity["text"].upper(), entity["label"], entity["score"]) for entity in entities if (entity["text"].upper() not in self.stopwords) and (len(entity["text"].upper()) <= num_len)]
                elif use_upper and not use_stopword:
                    return [(entity["text"].upper(), entity["label"], entity["score"]) for entity in entities if (len(entity["text"].upper()) <= num_len)]
                elif not use_upper and use_stopword:
                    return [(entity["text"], entity["label"], entity["score"]) for entity in entities if (entity["text"] not in self.stopwords) and (len(entity["text"]) <= num_len)]
                else:
                    return [(entity["text"], entity["label"], entity["score"]) for entity in entities if (len(entity["text"]) <= num_len)]
            except IndexError as e:
                return [(text, "Error", 0.0)]

        else:
            raise TypeError("The 'text' must be provided in str format. %s"%text) from None
        
class ZED():
    def __init__(self, category_df,
                 feature_extract_model='BM-K/KoSimCSE-roberta-multitask', 
                 cache_dir='../../models/huggingface',
                 use_cluster = None,
                 use_cuda = True):
        
        self.category_df = pd.read_csv(category_df, encoding="utf-8-sig", index_col=0)
        
        if use_cluster:
            self.category_df = self.category_df[self.category_df.cluster.isin(use_cluster)]
        self.category_df["category"] = self.category_df['대분류']+ " " + self.category_df['소분류']
        
        self.first_category_df = self.category_df[self.category_df.status==1]
        self.second_category_df = self.category_df[self.category_df.status==2]

        category_df_for_index = self.category_df.drop_duplicates(["category"])
        self.category_index = {i:(int(j) if pd.isna(k) else int(k)) for i,j,k in zip(category_df_for_index["category"], category_df_for_index["cluster"], category_df_for_index["new_cluster"])}
        self.category_index["기타"] = 0

        self.first_category = (self.first_category_df['대분류'] + " " + self.first_category_df['소분류']).tolist()
        self.second_category = (self.second_category_df['대분류'] + " " + self.second_category_df['소분류']).tolist()

        self.first_category_keyword = [i.split("/") for i in self.first_category_df["검색키워드"]]
        self.second_category_keyword = [" ".join(i.split("/")) for i in self.second_category_df["검색키워드"]]
        
        self.feature_model = AutoModel.from_pretrained(feature_extract_model, cache_dir=cache_dir) 
        self.feature_tokenizer = AutoTokenizer.from_pretrained(feature_extract_model, cache_dir=cache_dir)
    
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.use_cuda = False
        if use_cuda:
            if self.device == "cuda":
                self.use_cuda = True
                # 모델을 CUDA로 이동
                self.feature_model.to(self.device)
            else:
                print("cuda를 지원하지 않습니다.")

        self.inputs = self.feature_tokenizer(self.second_category_keyword, padding=True, truncation=True, return_tensors="pt")
        
        if self.use_cuda:
            self.inputs = {key: tensor.to(self.device) for key, tensor in self.inputs.items()}  # 모든 입력 텐서를 같은 장치로 이동합니다.
        self.label_embedding, _ = self.feature_model(**self.inputs, return_dict=False)


    def classification(self, text, etc_score=39):
        if isinstance(text, str):

            ### 외국어 분류
            if not calculate_korean_ratio(text, ratio=.0):
                return "해외채널", 9999, 0
            
            ### 선분류
            content = text.split('[SEP]')
            youtube_category = content[-1].strip()
            if youtube_category in ["Gaming"]:
                _content = " ".join(content).split()
            else:
                _content = content[0].strip().split()

            for i, keywords in enumerate(self.first_category_keyword):
                if len(set(_content) & set(keywords)) > 0:
                    return self.first_category[i], self.category_index[self.first_category[i]], 0

            ### 후분류
            ### 텍스트 임베딩
            try:
                inputs = self.feature_tokenizer(text, padding=True, truncation=True, return_tensors="pt")
                if self.use_cuda:
                    inputs = {key: tensor.to(self.device) for key, tensor in inputs.items()}  # 모든 입력 텐서를 같은 장치로 이동합니다.
                embedding = self.feature_model(**inputs, return_dict=False)
            except ValueError as e:
                return "기타", 0, 0
            sentences_similarity = dict()
            for i, ce in enumerate(self.label_embedding):
                sentences_similarity[self.second_category[i]] = float(self.cal_score(embedding[0][0][0], ce[0])[0][0].item())
            sentences_similarity = self.add_score(youtube_category, sentences_similarity)
            max_value = max(sentences_similarity.values())  # 가장 큰 값을 찾음

            ### etc_score 보다 낮으면 후분류 키워드가 제목안에 들어있으면 그걸로
            ### 아니면 기타로
            if max_value < etc_score:
                _content = content[0].strip().split()
                for i, keywords in enumerate(self.second_category_keyword):
                    if len(set(_content) & set(keywords)) > 0:
                        return self.second_category[i], self.category_index[self.second_category[i]], 0
                classification = "기타"
            else:
                classification = [key for key, value in sentences_similarity.items() if value == max_value][0]  # 가장 큰 값과 일치하는 모든 키를 찾음   
            return classification, self.category_index[classification], max_value
        
        if isinstance(text, list):
            results = dict()
            not_class_texts = list()
            not_class_categorys = list()

            for index, t in enumerate(text):
                class_status = False
                ### 외국어 분류
                if not calculate_korean_ratio(t, ratio=.0):
                    class_status = True
                    results[index] = ("해외채널", 9999, 0)
                
                ### 선분류
                content = t.split('[SEP]')
                youtube_category = content[-1].strip()
                if youtube_category in ["Gaming"]:
                    _content = " ".join(content).split()
                else:
                    _content = content[0].strip().split()

                for i, keywords in enumerate(self.first_category_keyword):
                    if len(set(_content) & set(keywords)) > 0:
                        class_status = True
                        results[index] = (self.first_category[i], self.category_index[self.first_category[i]], 0)
                        break
                if not class_status:
                    results[index] = ""
                    not_class_texts.append(t)
                    not_class_categorys.append(youtube_category)
                    
            
            if len(not_class_texts) == 0:
                return [value for value in results.values()]

            inputs = self.feature_tokenizer(not_class_texts, padding=True, truncation=True, return_tensors="pt")
            if self.use_cuda:
                inputs = {key: tensor.to(self.device) for key, tensor in inputs.items()}  # 모든 입력 텐서를 같은 장치로 이동합니다.
            embedding = self.feature_model(**inputs, return_dict=False)
            
            for i, e in enumerate(embedding[0]):
                sentences_similarity = dict()
                for j, ce in enumerate(self.label_embedding):
                    sentences_similarity[self.second_category[j]] = float(self.cal_score(e[0], ce[0])[0][0].item())
                sentences_similarity = self.add_score(not_class_categorys[i], sentences_similarity)
                max_value = max(sentences_similarity.values())  # 가장 큰 값을 찾음
                ### etc_score 보다 낮으면 후분류 키워드가 제목안에 들어있으면 그걸로
                ### 아니면 기타로

                if max_value < etc_score:
                    etc_status = True
                    _content = content[0].strip().split()
                    for j, keywords in enumerate(self.second_category_keyword):
                        if len(set(_content) & set(keywords)) > 0:
                            etc_status = False
                            for key, value in results.items():
                                if value == "":
                                    results[key] = (self.second_category[j], self.category_index[self.second_category[j]], 0)
                                    break
                            break
                    if etc_status:
                        for key, value in results.items():
                            if value == "":
                                results[key] = ("기타", 0, 0)
                                break
                else:
                    classification = [key for key, value in sentences_similarity.items() if value == max_value][0]  # 가장 큰 값과 일치하는 모든 키를 찾음   
                    for key, value in results.items():
                        if value == "":
                            results[key] = (classification, self.category_index[classification], max_value)
                            break

            return [value for value in results.values()]

    ### 거리계산 함수
    def cal_score(self, a, b):
        if len(a.shape) == 1: a = a.unsqueeze(0)
        if len(b.shape) == 1: b = b.unsqueeze(0)

        a_norm = a / a.norm(dim=1)[:, None]
        b_norm = b / b.norm(dim=1)[:, None]
        return torch.mm(a_norm, b_norm.transpose(0, 1)) * 100 


    ### 카테고리에 따른 추가점수
    def add_score(self, youtube_category, sentences_similarity):
        
        if youtube_category == "Music":
            for key in sentences_similarity:
                first_word = key.split()[0]  # 키를 split하여 첫 번째 원소를 추출
                if first_word == "음악":
                    sentences_similarity[key] += 5  # 값에 5를 더함

        elif youtube_category == "Gaming":
            for key in sentences_similarity:
                first_word = key.split()[0]  # 키를 split하여 첫 번째 원소를 추출
                if (first_word == "게임"):
                    sentences_similarity[key] += 5  # 값에 5를 더함

        elif youtube_category == "Sports":
            for key in sentences_similarity:
                first_word = key.split()[0]  # 키를 split하여 첫 번째 원소를 추출
                if first_word == "스포츠":
                    sentences_similarity[key] += 5  # 값에 5를 더함
        else:
            for key in sentences_similarity:
                first_word = key.split()[0]  # 키를 split하여 첫 번째 원소를 추출
                if first_word == "게임":
                    sentences_similarity[key] -= 3  # 값에 -3을 뺌

        return sentences_similarity
    
class PostProcessing():
    def __init__(self, josa_path = "../../data/josa/kor_josa.txt", 
                 stopwords_path="../../data/stopwords/stopwords_fin.txt", 
                 remove_pattern=None):
        if remove_pattern is None:
            self.remove_pattern =["제", "ep", "EP", "top", "TOP", "ch", "CH", "chapter", "T-", \
                                  "CHAPTER", "part", "PART", "에피소드", "episode", "EPISODE", "TO.", \
                                  "시즌", "season", "SEASON", "챕터", "level", "LEVEL", "레벨", "V-", \
                                  "day", "DAY", "톱", "탑", "파트", "no", "NO", "FT", "ft", "A", "NO:", \
                                  "ep.", "EP.", "top.", "TOP.", "ch.", "CH.", "chapter.", "CHAPTER.", \
                                  "part.", "PART.", "episode.", "EPISODE.", "season.", "SEASON.", \
                                  "level.", "LEVEL.", "lv", "LV", "lv.", "LV.", "no.", "NO.", "ft." \
                                  "FT.", "P", "P.", "P-", "p", "p.", "p-", "no-", "NO-", "T", "KY."]
        else:
            self.remove_pattern = remove_pattern
            
        self.josa_list = list()
        with open(josa_path, "r", encoding="utf-8-sig") as f:
            
            # 파일의 전체 라인 수를 가져옵니다.
            total_lines = sum(1 for _ in f)
            # 파일을 다시 처음부터 읽기 위해 커서를 처음으로 이동합니다.
            f.seek(0)
            
            # tqdm을 사용하여 진행 상황을 모니터링합니다.
            for line in tqdm(f, total=total_lines, desc="Reading Josa List"):
                text = line.strip()
                if text:
                    self.josa_list.append(text)
                    
        self.stopwords_list = set()
        with open(stopwords_path, "r", encoding="utf-8-sig") as f:
            
            # 파일의 전체 라인 수를 가져옵니다.
            total_lines = sum(1 for _ in f)
            # 파일을 다시 처음부터 읽기 위해 커서를 처음으로 이동합니다.
            f.seek(0)
            
            # tqdm을 사용하여 진행 상황을 모니터링합니다.
            for line in tqdm(f, total=total_lines, desc="Reading Stopwords List"):
                text = line.strip()
                if text:
                    self.stopwords_list.add(text)
        self.stopwords_list = list(self.stopwords_list)

    ### 후처리
    def post_processing(self, text, use_stopword=False):
        
        if isinstance(text, str):
            text = text.strip()
            # text = text.replace("'", "`")
            text = self.clean_text(text)
            
            if use_stopword:
                if text in self.stopwords_list:
                    return ""
            
            ### 숫자가 붙은것은 나머지 글자가 1개 이하면 빈셀 반환
            ### 숫자로 시작하는 단어면 빈셀 반환
            if len(re.sub(r'\d', '', text)) < 2 or not starts_with_korean_or_english(text):
                return ""
            
            for _word in self.remove_pattern:
                if self.find_pattern(text, _word):
                    return ""
            
            ### 영문이 없으면 조사제거 해서 반환
            else:
                for josa in self.josa_list:
                    if (text.endswith(josa)) and ((len(josa) >= 2) or (josa in ['는', '를', '의', '에', '와', '들'])):
                        return text[:-len(josa)]
                return text
    
        elif isinstance(text, list):
            result = list()
            # for word in [word.strip() for word in text if (len(re.sub(r'\d', '', word.strip())) >= 2 and starts_with_korean_or_english(word.strip())) and (len(word.strip()) < 10)]:
            for word in [word.strip() for word in text if (len(re.sub(r'\d', '', word.strip())) >= 2 and starts_with_korean_or_english(word.strip())) and (len(word.strip()) < 10) and all(not self.find_pattern(word, _word) for _word in self.remove_pattern)]:
                # word = word.replace("'", "`")
                word = self.clean_text(word)
            
                if word in self.stopwords_list:
                    continue
                
                for josa in self.josa_list:
                    have_josa = False
                    if (word.endswith(josa)) and ((len(josa) >= 2) or (josa in ['는', '를', '의', '에', '와', '들'])):
                        have_josa = True
                        result.append(word[:-len(josa)]) 
                        break
                if not have_josa:
                    result.append(word)
                
            return list(OrderedDict.fromkeys(result))
        
        else:
            raise TypeError("Only lists or strings are allowed as input %s"%text) 
        
    ### 특정 단어뒤에 숫자가 오는지 검사하는 함수
    def find_pattern(self, text, word):
        pattern = r'\b' + re.escape(word) + r'(\d+)'
        return re.search(pattern, text) is not None

    def clean_text(self, word):
        # 일부 특수문자 제거
        for char in ["`", "'", "#", ",", "]", "["]:
            word = word.replace(char, " ")
        # 두 개 이상의 공백을 하나의 공백으로 대체하고 양쪽 공백 제거
        word = re.sub(r'\s{2,}', ' ', word).strip()
        return word
        


if __name__ == "__main__":
    stopwords_path = "../../data/stopwords/stopwords_for_keyword.txt"
    use_cuda=False
    cache_dir = "../../models/huggingface"
    model_name = "taeminlee/gliner_ko"
    df_path = "../../data/video/video_20240415.csv"
    josa_path = "../../data/josa/kor_josa.txt"
    tta_labels = ["tokens", "artifacts", "person", "animal", "CIVILIZATION", "organization", \
            "phone number", "address", "passport number", "email", "credit card number", \
            "social security number", "health insurance id number", 'Business/organization', \
            "mobile phone number", "bank account number", "medication", "cpf", "driver's license number", \
            "tax identification number", "medical condition", "identity card number", "national id number", \
            "ip address", "email address", "iban", "credit card expiration date", "username", \
            "health insurance number", "student id number", "insurance number", \
            "landline phone number", "blood type", "cvv", \
            "digital signature", "social media handle", "license plate number", "cnpj", "postal code", \
            "passport_number", "vehicle registration number", "credit card brand", \
            "fax number", "visa number", "insurance company", "identity document number", \
            "national health insurance number", "cvc", "birth certificate number", "train ticket number", \
            "passport expiration date", "social_security_number", "EVENT", "STUDY_FIELD", "LOCATION", \
            "MATERIAL", "PLANT", "TERM", "THEORY", 'Analysis Requirement']

    # posp = PostProcessing(josa_path=josa_path)
    prep = PreProcessing(model=model_name, 
                        cache_dir=cache_dir,
                        tta_labels=tta_labels,
                        stopwords_path=stopwords_path,
                        use_cuda=use_cuda)

    # model = GLiNER.from_pretrained(model, cache_dir=cache_dir)
    # device = "cuda" if torch.cuda.is_available() else "cpu"
    # model.to(device)

    text = ["역전재판6] 제 5화 역전의 대혁명(2) #16 / 24.04.27 소니쇼 다시보기", 
            "메코클 레전드 무대 몰아보기(1~15화)",
            "유치원에 간 강아지에게 카메라를 달아봤어요! ｜ I put a camera on a dog that went to kindergarten!"]

    print(prep.use_norns(text[-1]))
    # text = [' '.join(remove_emojis(_text).strip()[:384].rsplit(' ', 1)[:-1]) for _text in text]
    
    # text = "[ENT] 역전재판6] 제 5화 역전의 대혁명(2) #16 / 24.04.27 소니쇼 다시보기 [ENT] 메코클 레전드 무대 몰아보기(1~15화) [ENT] 평화로운 선술집의 날 - 편안한 중세 음악, 판타지 음유시인/선술집 분위기, 켈트 음악 | Fantasy Bard/Tavern Music"
    # text = ' '.join(remove_emojis(text).strip()[:384].rsplit(' ', 1)[:-1])
    # for i in model.predict_entities(text, tta_labels):
    #     print(i)
    
    def cut_and_preprecessing(df, start_idx):
        all_use_text = str()
        while True:
            # print("start_idx", start_idx)
            end_idx = start_idx+1
            print("video_title:", df.iloc[start_idx].video_title)
            video_title = remove_emojis(df.iloc[start_idx].video_title).strip()
            
            video_tags = remove_emojis(" ".join(remove_special_characters(df.iloc[start_idx].video_tags, use_hashtag=False).split())).strip()
            hashtag = remove_emojis(" ".join(hashtag_extraction(df.iloc[start_idx].video_description).split('#'))).strip()
            use_text = "[ENT] "+video_title+" "+video_tags+" "+hashtag
     
            if len(all_use_text) + len(use_text) > 380:
                if end_idx == start_idx:
                    use_text += " [SEP]"
                    return use_text, end_idx
                break
            else:
                all_use_text += use_text+" "
                start_idx += 1
        all_use_text += "[SEP]"         
        return all_use_text, end_idx
