import time
import traceback
import yaml
import json


import os

import dataanalysis.callback
import dataanalysis.core as da
import dataanalysis.emerge as emerge
import dataanalysis.graphtools
from dataanalysis.printhook import log,log_logstash
from dataanalysis.caches.delegating import SelectivelyDelegatingCache

import dqueue
import imp

try:
    import sentryclient
except ImportError:
    def get_sentry_client():
        return None
else:
    def get_sentry_client():
        return sentryclient.get_client()

from dataanalysis.printhook import get_local_log
log=get_local_log("queue")


class QueueCache(SelectivelyDelegatingCache):
    delegate_by_default=True


    def __init__(self,queue_directory="/tmp/queue"):
        super(QueueCache, self).__init__()
        self.queue_directory=queue_directory
        self.queue = dqueue.from_uri(self.queue_directory)

        print("initialized dqueue:", self.queue)

    def delegate(self, hashe, obj):
        log(self,"\033[31mwill delegate",obj,"\033[0mas",hashe, level="top")
        task_data = dict(
            object_identity=obj.get_identity().serialize(),
        )

        if os.getenv('DDA_IDVERIFY_SIMPLIFY', 'no') == 'yes':
            pass
        else:
            try:
                emerge.verify_identity(da.DataAnalysisIdentity.from_dict(task_data['object_identity']))
                print("verify identity succeeded")
            except Exception:
                print("verify identity failed")
                raise

        if os.getenv('DDA_DEBUG_TASKDATA', 'no') == 'yes':
            # print("DDA_DEBUG_TASKDATA:\n\033[36m{}\033[0m".format(json.dumps(task_data, sort_keys=True, indent=4)))
            print(f"DDA_DEBUG_TASKDATA:\n\033[36mtotal assumptions {len(task_data['object_identity']['assumptions'])}\033[0m")
            print("DDA_DEBUG_TASKDATA:\n\033[36mall assumptions {}\033[0m".format(
                ("\n".join([f"\033[36m{a[0]}:\033[37m {a}\033[0m" for a in task_data['object_identity']['assumptions']]))
            ))

        r = None
        problems = []
        for i in range(20):
            try:
                r=self.queue.put(
                    task_data,
                    submission_data=dict(
                        callbacks=obj.callbacks,
                        request_origin="undefined",
                    ),
                )
                if getattr(r, 'status_code', 200) != 200:
                    problems.append(f"problematic response from put task {r}")
                    raise Exception(f"problematic response from put task {r}")
                break
            except Exception as e:
                log("problem putting task:", e)
                problems.append(f"problem putting task {e}, attempt {i}")
                time.sleep(2)

        if r is None:
            raise Exception(f"unable to put task in queue, problems {problems} task_data: {task_data}")
        
        log(self,"\033[31mdelegated",obj,"\033[0m with state", r['state'], level="top")

        if r['state'] == "done":
            #todo

            obj.process_hooks("top",
                              obj,message="task dependencies done while delegating, strange",
                              state="locked?", 
                              task_comment="dependencies done before task")

            task_key = r.get('key')

            log(self, "\033[31mtask dependencies done while delegating, strange!\033[0mas", level="top")
            log(self, f"\033[31mtask {task_key} {r}\033[0mas", level="top")

            self.queue.resubmit(scope="task", selector=task_key)
            #self.queue.remember(task_data) # really is a race condit: retry
            #raise Exception("delegated task already done: the task is done but cache was not stored and delegated requested: ",task_data['object_identity']['factory_name'])#
            #,task_data['object_identity']['assumptions'])

        r['task_data']=task_data
        return r

    def wipe_queue(self,kinds=["waiting"]):
        self.queue.wipe(kinds)


    def __repr__(self):
        return "["+self.__class__.__name__+": queue in \""+self.queue_directory+"\"]"


class QueueCacheWorker(object):

    def __repr__(self):
        return "[%s: %i]"%(self.__class__.__name__,id(self))

    def __init__(self,queue_directory="default", worker_id=None):
        self.queue_directory = queue_directory

        self.load_queue(worker_id)
        print("initialized dqueue:", self.queue)

    json_log_file = None

    _worker_metadata = None

    @property
    def worker_metadata(self):
        if self._worker_metadata is None:
            self._worker_metadata = json.loads(os.environ.get('DDA_WORKER_METADATA_JSON', '{}'))
            for k, v in os.environ.items():
                prefix = "DDA_WORKER_METADATA_"
                if k.startswith(prefix):
                    self._worker_metadata[k[len(prefix):].lower()] = v

            print("discovered worker metadata: ", self._worker_metadata)

        return self._worker_metadata

    def log_json(self, j):
        j_e = {
                **j,
               'worker_id': self.queue.worker_id,
               'worker_metadata': self.worker_metadata,
              }

        self.queue.log_task(json.dumps(j_e))
        if self.json_log_file:
            self.json_log_file.write(json.dumps(j_e)+"\n")
            self.json_log_file.flush()

    def load_queue(self, worker_id=None):
        self.queue = dqueue.from_uri(self.queue_directory, worker_id)


    def run_task(self, task):
        task_data=task.task_data
        log("emerging from object_identity",task_data['object_identity'])
        object_identity=da.DataAnalysisIdentity.from_dict(task_data['object_identity'])
        da.reset()

        imp.reload(dataanalysis.graphtools)
        log("fresh factory knows",da.AnalysisFactory.cache)
        
        t_start = time.time()
        self.log_json(dict(
                    origin="oda-worker",
                    action="worker_taking_task",
                    object_factory_name=object_identity.factory_name,
                ))

        log(object_identity)

        try:
            A=emerge.emerge_from_identity(object_identity)
        except Exception as e:
            log("\033[0mtask emergence failed!\033[0m", level="top")
            raise

        A._da_delegation_allowed=False

        dataanalysis.callback.Callback.set_callback_accepted_classes([da.byname(object_identity.factory_name).__class__])
        
        t_start_get = time.time()
        self.queue.log_task(json.dumps(dict(
                    origin="oda-worker",
                    action="worker_emerged_task",
                    object_factory_name=object_identity.factory_name,
                    worker_id=self.queue.worker_id,
                )))


        for url in task.submission_info.get('callbacks', []):
            log("setting object callback",A,url)
            A.set_callback(url)

        log("emerged object:",A)
        

        request_root_node=getattr(A, 'request_root_node', False)
        if request_root_node:
            final_state = "done"
        else:
            final_state = "task_done"

        try:
            result=A.get(requested_by=[repr(self)],isolated_directory_key=task.key,isolated_directory_cleanup=True)
            A.raise_stored_exceptions()
        except da.AnalysisException as e:
            A.process_hooks("top", A, message="task complete", state=final_state, task_comment="completed with failure "+repr(e))
            A.process_hooks("top", A, message="analysis exception", exception=repr(e),state="node_analysis_exception")
        except da.AnalysisDelegatedException as delegation_exception:
            final_state = "task_done"
            log("delegated dependencies:",delegation_exception)
            A.process_hooks("top",A,message="task dependencies delegated",state=final_state, task_comment="task dependencies delegated",delegation_exception=repr(delegation_exception))
            raise
        except dqueue.TaskStolen:
            raise
        except Exception as e:
            A.process_hooks("top", A, message="task complete", state=final_state, task_comment="completed with unexpected failure "+repr(e))

            client=get_sentry_client()
            if client is not None:
                client.captureException()

            self.log_json(dict(
                            origin="oda-worker",
                            action="run_task_exception",
                            object_factory_name=object_identity.factory_name,
                            object_fullname=str(A),
                            tspent_s=time.time()-t_start,
                            tspent_get_s=time.time()-t_start_get,
                         ))

            raise
        else:
            A.process_hooks("top",A,message="task complete",state=final_state, task_comment="completed with success")
            self.log_json(dict(
                            origin="oda-worker",
                            action="run_task_complete",
                            object_factory_name=object_identity.factory_name,
                            object_fullname=str(A),
                            tspent_s=time.time()-t_start,
                            tspent_get_s=time.time()-t_start_get,
                         ))
            return result


    def run_once(self):
        self.run_all()

    def set_worker_knowledge(self, w):
        self._worker_knowledge = w

    @property
    def worker_knowledge(self):
        return getattr(self, "_worker_knowledge", {})

    def run_all(self, limit_tasks=1, limit_time_seconds=0, wait=10):
        log_logstash("worker", message="worker starting", worker_event="starting")
        worker_tasks=0

        print('worker_knowledge:', self.worker_knowledge)

        worker_t0 = time.time()

        try:
            worker_heartrate_skip=int(os.environ.get("WORKER_HEARTRATE_SKIP","0"))
        except Exception as e:
            log("problem determining worker heartrate",os.environ.get("WORKER_HEARTRATE_SKIP","UNDEFINED"))
            worker_heartrate_skip=0

        while True:
            if worker_heartrate_skip>0 and worker_tasks%worker_heartrate_skip==0:
                log_logstash("worker", message="worker heart rate "+repr(self.queue.info), queue_info=self.queue.info,worker_age=worker_tasks)
            worker_tasks+=1

            if limit_tasks is not None and limit_tasks>0 and worker_tasks>limit_tasks:
                log(f"\033[31mstopping worker, it completed {worker_tasks} > {limit_tasks} tasks\033[0m")
                break

            worker_age_seconds = time.time() - worker_t0

            if limit_time_seconds is not None and limit_time_seconds>0 and worker_age_seconds > limit_time_seconds:
                log(f"\033[31mstopping worker due to old age, {worker_age_seconds} > {limit_time_seconds} seconds\033[0m")
                break

            try:
                log("trying to get a task from", self.queue)
                print("trying to get a task from", self.queue)
                
                task = self.queue.get(
                        worker_knowledge=self.worker_knowledge,
                        only_users=os.environ.get('DDA_ONLY_USERS', 'all')
                    )

                print(f"\033[031mgot task: {repr(task):.200s}...\033[0m")
                print(f"\033[031mgot task data: {repr(task.task_data):.200s}... \033[0m")
                open('object_identity.json', 'w').write(json.dumps(task.task_data['object_identity']))
                log_logstash("worker",message="worker taking task",origin="dda_worker",worker_event="taking_task",target=task.task_data['object_identity']['factory_name'])
            except dqueue.TaskStolen:
                time.sleep(wait)
                continue
            except dqueue.Empty:
                print("queue empty")
                time.sleep(wait)
                continue

            try:
                self.run_task(task)
            except dqueue.TaskStolen as e:
                log("task stolen, whatever",e)
            except da.AnalysisDelegatedException as delegation_exception:
                log("found delegated dependencies:", da.repr_short(delegation_exception.delegation_states), level='top')
                task_dependencies = [d['task_data'] for d in delegation_exception.delegation_states]
                #locked_task=dqueue.Task.from_file(self.queue.put(task.task_data)['fn'])
                #assert task.filename_key == locked_task.filename_key

                self.queue.task_locked(depends_on=task_dependencies)
                log("task locked",task)
                log_logstash("worker", message="worker task locked", origin="dda_worker", worker_event="task_done",
                             target=task.task_data['object_identity']['factory_name'])
            except Exception as e:
                log("task failed:",e, level="top")
                log_logstash("worker",message="worker task failed",origin="dda_worker",worker_event="task_failed",target=task.task_data['object_identity']['factory_name'])
                client=get_sentry_client()
                if client is not None:
                    client.captureException()
                traceback.print_exc()

                def update(task):
                    task.execution_info = dict(
                        status="failed",
                        exception=dict(
                            exception_class=e.__class__.__name__,
                            exception_message=getattr(e, 'message', repr(e)),
                            exception_args=e.args,
                            formatted_exception=traceback.format_exc(),
                        ),
                    )

                #TODO: ddasentry
         #       A.process_hooks("top",A,message="task dependencies delegated",state=final_state, task_comment="task dependencies delegated")

                self.queue.task_failed(update)
            else:
                log("DONE!")
                log_logstash("worker",message="worker task done",origin="dda_worker",worker_event="task_done",target=task.task_data['object_identity']['factory_name'])
                self.queue.task_done()

    def queue_status(self):
        return ""

        def get_task_attributes(task):
            for k in task.task_data['object_identity']['assumptions']:
                if isinstance(k,tuple) and k[0] == "ScWData":
                    return {'scw':k[1]['_da_stored_string_input_scwid']}

        r="="*80+"\n"
        for kind in "failed","done","locked","waiting","running":
            r+=kind+":"
            tasks=self.queue.list(kind)
            r += "(%i) \n" % len(tasks)
            for task_fn in tasks:
                try:
                    task=dqueue.Task.from_file(self.queue.queue_dir(kind)+"/"+task_fn)
                except Exception as e:
                    r+="> unreadable"
                else:
                    r+="> "+task_fn+": "+task.task_data['object_identity']['factory_name'] + ";"+repr(get_task_attributes(task))+"\n"
                    if task.depends_on is not None:
                        for dependency in task.depends_on:
                            r+="> > "+dependency['object_identity']['factory_name'] + ";"+repr(get_task_attributes(task)) + "\n"
            r+="\n\n"
        return r


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("queue", nargs='?', default=os.environ.get("ODAHUB", None))
    parser.add_argument('-V', dest='very_verbose',  help='...',action='store_true', default=False)
    parser.add_argument('-B', dest='limit_tasks', help='...', type=int, default=1)
    parser.add_argument('-t', dest='limit_time_seconds', help='...', type=int, default=0)
    parser.add_argument('-w', dest='watch', type=int, help='...', default=0)
    parser.add_argument('-W', dest='watch_closely', type=int, help='...', default=0)
    parser.add_argument('-d', dest='delay', type=int, help='...', default=10)
    parser.add_argument('-k', dest='worker_knowledge_yaml', type=str, help='...', default=None)
    parser.add_argument('-n', dest='worker_id', type=str, help='...', default=None)
    parser.add_argument('--json-log-file', dest='json_log_file', type=str, help='...', default=None)

    args=parser.parse_args()

    if args.very_verbose:
        dataanalysis.printhook.global_permissive_output=True
        da.debug_output()

    qcworker = QueueCacheWorker(args.queue, args.worker_id)

    if args.json_log_file:
        qcworker.json_log_file = open(args.json_log_file, "at")


    if args.worker_knowledge_yaml is not None:
        qcworker.set_worker_knowledge(yaml.load(open(args.worker_knowledge_yaml), Loader=yaml.FullLoader))

    if args.watch_closely > 0:
        while True:
            log(qcworker.queue_status())
            time.sleep(args.watch_closely)
    elif args.watch>0:
        while True:
            log(qcworker.queue.info)
            time.sleep(args.watch)
    else:
        qcworker.run_all(
                    limit_tasks=args.limit_tasks,
                    limit_time_seconds=args.limit_time_seconds,
                    wait=args.delay
                )





if __name__ == "__main__":
    main()
