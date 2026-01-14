from multiprocessing import Process
import signal
import dkvs
import time
import argparse


def launch_cluster():
    procs = []
    orche = Process(target=dkvs.run_orche, args=[2, 8000], daemon=True)
    orche.start()
    time.sleep(0.2)
    procs.append(orche)
    for i in range(2):
        store = Process(
            target=dkvs.run_store, args=["http://localhost:8000/"], daemon=True
        )
        store.start()
        procs.append(store)
    return procs


def wait_for_terminate(procs):
    try:
        signal.sigwait({signal.SIGINT, signal.SIGTERM})
    except KeyboardInterrupt:
        pass
    print("\nShutting down cluster…")
    for p in procs:
        if p.is_alive():
            p.terminate()
            p.join(timeout=2)
            if p.is_alive():
                p.kill()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "mode", type=str, default=f"cluster", choices=["cluster", "cli"]
    )
    args = parser.parse_args()
    if args.mode == "cluster":
        procs = launch_cluster()
        wait_for_terminate(procs)
    else:
        dkvs.client_repl(dkvs.Client())
