import os
import sys
import git
import json
import subprocess


def main():
    repo = git.Repo('./')
    commit_files = []
    repo_diffs = repo.head.commit.diff('HEAD~1')
    for item in repo_diffs:
        it = str(item)
        it = it[:it.find("\n")]
        commit_files.append(it)

    action = check_commit_message(repo)
    dsp_calls(action, commit_files)


def check_commit_message(repo):
    com_message = str(repo.head.commit.message)
    if com_message.find("Action: Save") != -1:
        print("Found Action Save. Saving all the pipelines changed in the latest commit")
        return "save"
    elif com_message.find("Action: Activate") != -1:
        print("Found Action Activate. Activating all the pipelines changed in the latest commit")
        return "activate"
    return "false"


def dsp_calls(action, commit_files):
    for file in commit_files:
        print("Running changes in file " + file)
        file1 = open("./" + file, 'r')
        lines = file1.readlines()
        for line in lines:
            pipeline = json.loads(line)
            compile_spl_response = compile_pipeline(str(pipeline["spl"]).replace('"', '\\"'))
            if compile_spl_response.find("HTTPStatusCode") != -1:
                print(compile_spl_response)
                sys.exit(1)
            save_response = update_pipeline(compile_spl_response, str(pipeline["name"]), str(pipeline["id"]))
            print(save_response)
            if save_response.find("HTTPStatusCode") != -1:
                sys.exit(1)
            data = json.loads(save_response)
            if action == "activate":
                activate_pipeline(str(data["id"]), str(data["status"]))


def compile_pipeline(spl):
    spl_file_name = "pipeline.spl"
    spl_file_pipeline = open(spl_file_name, "a")
    spl_file_pipeline.write("{\"spl\": \"" + spl + "\"}")
    spl_file_pipeline.close()
    print("Saving pipeline:")
    compile_output = subprocess.run(
        ['scloud',
         'streams',
         "compile",
         "--validate",
         "true",
         "--input-datafile",
         spl_file_name
         ],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL)
    os.remove(spl_file_name)
    if str(compile_output.stderr) != "None":
        print("Error in executing the compile")
        print(compile_output.stderr)
        sys.exit(1)
    return compile_output.stdout.decode("utf-8")


def update_pipeline(compile_spl_response, pipeline_name, pipeline_id):
    file_name = "compiled.json"
    file_compiled_pipeline = open(file_name, "a")
    file_compiled_pipeline.write(compile_spl_response)
    file_compiled_pipeline.close()
    pipeline_post = subprocess.run(
        ['scloud',
         'streams',
         "update-pipeline",
         "--input-datafile",
         file_name,
         "--name",
         pipeline_name,
         "--id",
         pipeline_id
         ],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL)
    os.remove(file_name)
    if str(pipeline_post.stderr) != "None":
        print("Error in executing the pipeline post")
        print(pipeline_post.stderr)
        sys.exit(1)
    return pipeline_post.stdout.decode("utf-8")


def activate_pipeline(pipeline_id, status):
    if status == "ACTIVATED":
        print("Pipeline already active. Reactivating " + str(pipeline_id))
        pipeline_reactivate = subprocess.run(
            ['scloud',
             'streams',
             "reactivate-pipeline",
             "--id",
             pipeline_id
             ],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL)
        if str(pipeline_reactivate.stderr) != "None":
            print("Error in executing the pipeline post")
            print(pipeline_reactivate.stderr)
            sys.exit(1)
        activate_response = pipeline_reactivate.stdout.decode("utf-8")
        print(activate_response)
        if activate_response.find("HTTPStatusCode") != -1:
            sys.exit(1)
    else:
        print("Activating Pipeline: ", str(pipeline_id))
        pipeline_activate = subprocess.run(
            ['scloud',
             'streams',
             "activate-pipeline",
             "--activate-latest-version",
             "true",
             "--id",
             pipeline_id
             ],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL)
        if str(pipeline_activate.stderr) != "None":
            print("Error in executing the pipeline post")
            print(pipeline_activate.stderr)
            sys.exit(1)
        activate_response = pipeline_activate.stdout.decode("utf-8")
        print(activate_response)
        if activate_response.find("HTTPStatusCode") != -1:
            sys.exit(1)


if __name__ == '__main__':
    main()
