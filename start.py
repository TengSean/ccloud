import luigi
from luigi.contrib.simulate import RunAnywayTarget

import os, json

import spacy

class Tokenizer_Task(luigi.Task):
    pipline_path = luigi.Parameter()
    out_path = luigi.Parameter()

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
        with open(self.pipline_path, 'r') as f:
            DO_OR_NOT = json.load(f)['pipline']['Tok']
        if DO_OR_NOT == 1:
#             print('In the if')
            paragraph = in_out_json['content'][0]['sentence']['origin']
#             print('paragraph')
            in_out_json['content'][0]['sentence']['Tok'] = self.Tokenizer_handler(paragraph)
            print(in_out_json)
            with open(self.out_path, 'w') as f:
                json.dump(in_out_json, f)
        self.output().done()

    def output(self, ):
        return RunAnywayTarget(self)
    
class Dep_Task(luigi.Task):
    pipline_path = luigi.Parameter()
    out_path = luigi.Parameter()

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
        with open(self.pipline_path, 'r') as f:
            DO_OR_NOT = json.load(f)['pipline']['Tok']
        if DO_OR_NOT == 1:
#             print('In the if')
            paragraph = in_out_json['content'][0]['sentence']['origin']
#             print('paragraph')
            in_out_json['content'][0]['sentence']['Dep'] = self.Dep_handler(paragraph)
            print(in_out_json)
            with open(self.out_path, 'w') as f:
                json.dump(in_out_json, f)
        self.output().done()

    def output(self, ):
        return RunAnywayTarget(self)
    
class Ner_Task(luigi.Task):
    pipline_path = luigi.Parameter()
    out_path = luigi.Parameter()

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
        with open(self.pipline_path, 'r') as f:
            DO_OR_NOT = json.load(f)['pipline']['Tok']
        if DO_OR_NOT == 1:
#             print('In the if')
            paragraph = in_out_json['content'][0]['sentence']['origin']
#             print('paragraph')
            in_out_json['content'][0]['sentence']['Ner'] = self.Ner_handler(paragraph)
            print(in_out_json)
            with open(self.out_path, 'w') as f:
                json.dump(in_out_json, f)
        self.output().done()

    def output(self, ):
        return RunAnywayTarget(self)
    
    
    
class Tag_Task(luigi.Task):
    pipline_path = luigi.Parameter()
    out_path = luigi.Parameter()

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
        with open(self.pipline_path, 'r') as f:
            DO_OR_NOT = json.load(f)['pipline']['Tok']
        if DO_OR_NOT == 1:
#             print('In the if')
            paragraph = in_out_json['content'][0]['sentence']['origin']
#             print('paragraph')
            in_out_json['content'][0]['sentence']['Tag'] = self.Tag_handler(paragraph)
            print(in_out_json)
            with open(self.out_path, 'w') as f:
                json.dump(in_out_json, f)
        self.output().done()

    def output(self, ):
        return RunAnywayTarget(self)
    
    
    
class Pos_Task(luigi.Task):
    pipline_path = luigi.Parameter()
    out_path = luigi.Parameter()

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
        with open(self.pipline_path, 'r') as f:
            DO_OR_NOT = json.load(f)['pipline']['Tok']
        if DO_OR_NOT == 1:
#             print('In the if')
            paragraph = in_out_json['content'][0]['sentence']['origin']
#             print('paragraph')
            in_out_json['content'][0]['sentence']['Pos'] = self.Pos_handler(paragraph)
            print(in_out_json)
            with open(self.out_path, 'w') as f:
                json.dump(in_out_json, f)
        self.output().done()

    def output(self, ):
        return RunAnywayTarget(self)
    
    
    
class Entry(luigi.Task):
    input_path = luigi.Parameter(default='this is json')
    base_path = './data/'

    def requires(self, ):
        return [Tokenizer_Task(pipline_path = self.pipline_path, out_path= self.output_path), 
                Pos_Task(pipline_path = self.pipline_path, out_path = self.output_path), 
                Tag_Task(pipline_path = self.pipline_path, out_path = self.output_path), 
                Ner_Task(pipline_path = self.pipline_path, out_path = self.output_path),
                Dep_Task(pipline_path = self.pipline_path, out_path = self.output_path)]
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sample_path = './data/sample_output.json'
        self.pipline_path = os.path.join(
                            os.path.join(self.base_path, 'input_pipline'),
                            self.input_path
                        )
        self.paragraph_path = os.path.join(
                            os.path.join(self.base_path, 'input_paragraph'),
                            self.input_path
                        )
        self.output_path = os.path.join(
                            os.path.join(self.base_path, 'output'),
                            self.input_path
                        )
        print('start writing json')
        with open(self.sample_path, 'r') as f:
            sample = json.load(f)
        with open(self.paragraph_path, 'r') as f:
            paragraph_json = json.load(f)
        sample['content'][0]['sentence']['origin'] = paragraph_json['content'][0]['sentence']['origin']
        with open(self.output_path, 'w') as f:
            json.dump(sample, f)
        with open(self.output_path, 'r') as f:
            sample = json.load(f)
        print(json.dumps(sample, indent=2))
    def run(self, ):
        print('Finish')

        
        
        
        
    
if __name__ == '__main__':
    luigi.run()