import luigi
from luigi.contrib.simulate import RunAnywayTarget

import os, json

import spacy

class Tokenizer_Task(luigi.Task):
    out_path = luigi.Parameter()
    threshold = luigi.Parameter()
    def Tokenizer_handler(self, paragraph):
        import en_core_web_sm
        nlp = en_core_web_sm.load()
        doc = nlp(paragraph)
        return [str(token) for token in doc ]
    
    def check_json(self, ):
        pass
    
    def run(self, ):
        with open(self.out_path, 'r') as f:
            in_out_json = json.load(f)
        if int(self.threshold) == 1:
#             print('In the if')
            paragraph = in_out_json['content'][0]['sentence']['origin']
#             print('paragraph')
            in_out_json['content'][0]['sentence']['Tok'] = self.Tokenizer_handler(paragraph)
#             print(in_out_json)
            with open(self.out_path, 'w') as f:
                json.dump(in_out_json, f)
        self.output().done()

    def output(self, ):
        return RunAnywayTarget(self)
    
class Dep_Task(luigi.Task):
    out_path = luigi.Parameter()
    threshold = luigi.Parameter()

    def Dep_handler(self, paragraph):
        import en_core_web_sm
        nlp = en_core_web_sm.load()
        doc = nlp(paragraph)
#         return [str(token) for token in doc ]
        return_dict = {}
        for token in doc:
            return_dict[str(token.text)] = str(token.dep_)
        return return_dict
    
    def check_json(self, ):
        pass
    
    def run(self, ):
        with open(self.out_path, 'r') as f:
            in_out_json = json.load(f)
        if int(self.threshold) == 1:
#             print('In the if')
            paragraph = in_out_json['content'][0]['sentence']['origin']
#             print('paragraph')
            in_out_json['content'][0]['sentence']['Dep'] = self.Dep_handler(paragraph)
#             print(in_out_json)
            with open(self.out_path, 'w') as f:
                json.dump(in_out_json, f)
        self.output().done()

    def output(self, ):
        return RunAnywayTarget(self)
    
class Ner_Task(luigi.Task):
    out_path = luigi.Parameter()
    threshold = luigi.Parameter()

    def Ner_handler(self, paragraph):
        import en_core_web_sm
        nlp = en_core_web_sm.load()
        doc = nlp(paragraph)
#         return [str(token) for token in doc ]
        return_dict = {}
        for ent in doc.ents:
            return_dict[str(ent.text)] = str(ent.label_)
        return return_dict
    
    def check_json(self, ):
        pass
    
    def run(self, ):
        with open(self.out_path, 'r') as f:
            in_out_json = json.load(f)
        if int(self.threshold) == 1:
#             print('In the if')
            paragraph = in_out_json['content'][0]['sentence']['origin']
#             print('paragraph')
            in_out_json['content'][0]['sentence']['Ner'] = self.Ner_handler(paragraph)
#             print(in_out_json)
            with open(self.out_path, 'w') as f:
                json.dump(in_out_json, f)
        self.output().done()

    def output(self, ):
        return RunAnywayTarget(self)
    
    
    
class Tag_Task(luigi.Task):
    out_path = luigi.Parameter()
    threshold = luigi.Parameter()

    def Tag_handler(self, paragraph):
        import en_core_web_sm
        nlp = en_core_web_sm.load()
        doc = nlp(paragraph)
#         return [str(token) for token in doc ]
        return_dict = {}
        for token in doc:
            return_dict[str(token.text)] = str(token.tag_)
        return return_dict
    
    def check_json(self, ):
        pass
    
    def run(self, ):
        with open(self.out_path, 'r') as f:
            in_out_json = json.load(f)
        if int(self.threshold) == 1:
#             print('In the if')
            paragraph = in_out_json['content'][0]['sentence']['origin']
#             print('paragraph')
            in_out_json['content'][0]['sentence']['Tag'] = self.Tag_handler(paragraph)
#             print(in_out_json)
            with open(self.out_path, 'w') as f:
                json.dump(in_out_json, f)
        self.output().done()

    def output(self, ):
        return RunAnywayTarget(self)
    
    
    
class Pos_Task(luigi.Task):
    out_path = luigi.Parameter()
    threshold = luigi.Parameter()

    def Pos_handler(self, paragraph):
        import en_core_web_sm
        nlp = en_core_web_sm.load()
        doc = nlp(paragraph)
#         return [str(token) for token in doc ]
        return_dict = {}
        for token in doc:
            return_dict[str(token.text)] = str(token.pos_)
        return return_dict
    
    def check_json(self, ):
        pass
    
    def run(self, ):
        with open(self.out_path, 'r') as f:
            in_out_json = json.load(f)
        if int(self.threshold) == 1:
#             print('In the if')
            paragraph = in_out_json['content'][0]['sentence']['origin']
#             print('paragraph')
            in_out_json['content'][0]['sentence']['Pos'] = self.Pos_handler(paragraph)
#             print(in_out_json)
            with open(self.out_path, 'w') as f:
                json.dump(in_out_json, f)
        self.output().done()

    def output(self, ):
        return RunAnywayTarget(self)
    
    
    
class Entry(luigi.Task):
    input_path = luigi.Parameter(default='this is json')
    input_paragraph = luigi.Parameter()
    input_pipline = luigi.Parameter()
    base_path = './data/'

#     def pipline_parser(self, pipline_str):
#         pass

    def requires(self, ):
        return [Tokenizer_Task(out_path= self.output_path, threshold = self.pipline_dict['Tok']), 
                Pos_Task(out_path = self.output_path, threshold = self.pipline_dict['Pos']), 
                Tag_Task(out_path = self.output_path, threshold = self.pipline_dict['Tag']), 
                Ner_Task(out_path = self.output_path, threshold = self.pipline_dict['Ner']),
                Dep_Task(out_path = self.output_path, threshold = self.pipline_dict['Dep'])]
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sample_path = './data/sample_output.json'
#         self.pipline_path = os.path.join(
#                             os.path.join(self.base_path, 'input_pipline'),
#                             self.input_path
#                         )
#         self.paragraph_path = os.path.join(
#                             os.path.join(self.base_path, 'input_paragraph'),
#                             self.input_path
#                         )
        self.output_path = os.path.join(
                            os.path.join(self.base_path, 'output'),
                            self.input_path
                        )
        print('start writing json')
        self.pipline_dict = json.loads(self.input_pipline)
#         print(type(self.input_pipline), self.input_pipline)
#         print(self.pipline_dict.keys())
        with open(self.sample_path, 'r') as f:
            sample = json.load(f)
#         with open(self.paragraph_path, 'r') as f:
#             paragraph_json = json.load(f)
#         sample['content'][0]['sentence']['origin'] = paragraph_json['content'][0]['sentence']['origin']

        sample['content'][0]['sentence']['origin'] = self.input_paragraph
        with open(self.output_path, 'w') as f:
            json.dump(sample, f)
        with open(self.output_path, 'r') as f:
            sample = json.load(f)
#         print(json.dumps(sample, indent=2))
        
    def run(self, ):
        print('Finish')

        
        
        
        
    
if __name__ == '__main__':
    luigi.run()